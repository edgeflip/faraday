"""Extension to the boto Item and framework for the class-based definition of
DynamoDB tables and documents.

"""
import copy
import itertools

from boto.dynamodb2 import fields as basefields
from boto.dynamodb2 import items as baseitems

from . import loading
from . import types
from . import utils
from .fields import ItemField
from .manager import AbstractLinkedItemManager, ItemManager
from .table import Table
from .utils import epoch


# Define framework for the definition of dynamo tables and #
# data interactions around (an extension of) the boto Item #


class ItemMeta(object):
    """Item definition metadata"""
    # User-available options & default values #
    DEFAULTS = {
        'abstract': False,
        'allow_undeclared_fields': False,
        'app_name': None,
        'indexes': (),
        'undeclared_data_type': None,

        # Will default to lowercased, pluralized version of class name:
        'table_name': None,
    }

    def __init__(self, name, keys=None, links=None, managers=None, user=None):
        # Set options:
        vars(self).update(self.DEFAULTS)
        if user:
            vars(self).update(
                (key, value) for (key, value) in vars(user).items()
                if not key.startswith('__')
            )

        self.item_name = name

        if self.table_name:
            if self.abstract:
                raise TypeError("Abstract Item may not define table")
        elif not self.abstract:
            self.table_name = utils.camel_to_underscore(name)
            if not self.table_name.endswith('s'):
                self.table_name += 's'
            if self.app_name:
                self.table_name = '.'.join([self.app_name, self.table_name])

        self.keys = keys or {}
        self.links = links or {}
        self.managers = managers or {}

        # Set late on item declaration by set_properties:
        self.fields = self.link_keys = self.signed = None

    def __repr__(self):
        return "<{}: {}{}>".format(type(self).__name__,
                                   self.signed,
                                   ' (abstract)' if self.abstract else '')

    def set_properties(self):
        self.fields = self.links.copy()
        self.fields.update(self.keys)

        self.link_keys = {}
        for (link_name, link_field) in self.links.items():
            for db_key_item in link_field.db_key:
                self.link_keys[db_key_item] = link_name

        self.signed = '.'.join(part for part in (self.app_name, self.item_name) if part)


class ItemDoesNotExist(LookupError):
    pass


class MultipleItemsReturned(LookupError):
    pass


class DeclarativeItemBase(type):
    """Metaclass which defines subclasses of Item based on their declarations."""
    _default_manager = 'items'
    _update_field = 'updated'

    def __new__(mcs, name, bases, attrs):
        parents = [base for base in bases if isinstance(base, DeclarativeItemBase)]
        if not parents:
            # This is the base class
            return super(DeclarativeItemBase, mcs).__new__(mcs, name, bases, attrs)

        module = attrs.pop('__module__')
        attr_meta = attrs.pop('Meta', None)
        item_meta = ItemMeta(name, user=attr_meta)
        cls = super(DeclarativeItemBase, mcs).__new__(mcs, name, bases, {
            '__module__': module,
            '_meta': item_meta,
        })

        # Set declared values:
        for (key, value) in attrs.items():
            try:
                contributor = value.contribute_to_class
            except AttributeError:
                setattr(cls, key, value)
            else:
                contributor(cls, key)

        places_to_check = (item_meta.keys, item_meta.links, item_meta.managers)

        # Reset inherited values as copies:
        for parent in parents:
            if parent is Item:
                continue

            for (key, value) in itertools.chain(parent._meta.fields.items(),
                                                parent._meta.managers.items()):
                if key not in attrs and not any(key in place for place in places_to_check):
                    # key not overwritten in defn nor by another parent
                    copy.copy(value).contribute_to_class(cls, key)

        # Set defaults:
        if not item_meta.abstract and mcs._update_field and not hasattr(cls, mcs._update_field):
            ItemField(data_type=types.DATETIME).contribute_to_class(cls, mcs._update_field)

        if not any(mcs._default_manager in place for place in places_to_check):
            ItemManager().contribute_to_class(cls, mcs._default_manager)

        item_meta.set_properties()
        if item_meta.abstract:
            return cls

        # Set Item-specific exceptions:
        concrete_parents = [parent for parent in parents
                            if hasattr(parent, '_meta') and not parent._meta.abstract]
        cls.DoesNotExist = type(
            'DoesNotExist',
            (tuple(parent.DoesNotExist for parent in concrete_parents) or (ItemDoesNotExist,)),
            {'__module__': module}
        )
        cls.MultipleItemsReturned = type(
            'MultipleItemsReturned',
            (tuple(parent.MultipleItemsReturned for parent in concrete_parents) or (MultipleItemsReturned,)),
            {'__module__': module}
        )

        # Schema must be [HashKey, RangeKey]:
        schema = []
        for (field_name, field) in item_meta.keys.items():
            internal_field = field.make_internal(field_name)
            if isinstance(internal_field, basefields.HashKey):
                schema.insert(0, internal_field)
            elif isinstance(internal_field, basefields.RangeKey):
                schema.append(internal_field)

        # Ensure managers set up with reference to table (and class):
        item_table = Table(
            table_name=item_meta.table_name,
            item=cls,
            schema=schema,
            indexes=item_meta.indexes,
        )
        for manager in item_meta.managers.itervalues():
            manager.table = item_table

        # Notify listeners:
        loading.item_declared.send(sender=cls)

        return cls


class Item(baseitems.Item):
    """Extention to the boto Item, allowing for definition of Table schema, additional
    field-level validation and data conversion, and table-specific objectification.

    Items and their Tables may be defined as simply as::

        class User(Item):

            username = HashKeyField(data_type=NUMBER)

    and then their tables interacted with as::

        User.items.get_item(username='johndoe')

    The default item "manager" may be overridden, additional managers specified, and
    Item options may be defined via a `Meta` class::

        class User(Item):

            username = HashKeyField(data_type=NUMBER)

            items = MyItemManager()
            fancyitems = MyFancyItemManager()

            class Meta(object):
                allow_undeclared_fields = True

    """
    __metaclass__ = DeclarativeItemBase
    get_dynamizer = types.Dynamizer

    def __init__(self, data=None, loaded=False, **kwdata):
        if type(self) is Item or self._meta.abstract:
            raise TypeError("Can't instantiate abstract Item")

        self.table = type(self).items.table
        self._dynamizer = self.get_dynamizer()
        self._loaded = loaded

        # Clean data and gather links
        self._data = {}
        self._orig_data = {}
        data = {} if data is None else dict(data)
        data.update(kwdata)
        linked_data = []
        for (key, value) in data.items():
            if key in self._meta.links:
                if loaded:
                    # Allowing this would cause weird issues with _orig_data,
                    # (and wouldn't make sense anyway):
                    raise TypeError("Items loaded from database cannot populate link fields")
                linked_data.append((key, value))
            else:
                self._data[key] = self._pre_set(key, value)
                if loaded:
                    self._orig_data[key] = self._pre_set(key, value, lossy=False)

        # Apply linked objects
        for (key, value) in linked_data:
            setattr(self, key, value)

    def __repr__(self):
        pk = self.pk
        keys = ', '.join(unicode(key) for key in pk)
        if len(pk) > 1:
            keys = "({})".format(keys)
        return "<{name}: {keys}>".format(name=self.__class__.__name__, keys=keys)

    def __getstate__(self):
        """Return the Item object state for pickling."""
        # It's probably worthwhile for ReverseLinkFieldProperty to cache
        # LinkedItemManagers on the instance; but, not worthwhile, for the time
        # being anyway, to support pickling of instances of these manufactured
        # classes.
        return {key: value for (key, value) in vars(self).items()
                if not isinstance(value, AbstractLinkedItemManager)}

    @property
    def pk(self):
        """The Item's signature in key-less, hashable form."""
        return tuple(self.get_keys().values())

    @property
    def document(self):
        """The Item's data excluding its signature."""
        meta_fields = set(itertools.chain(self.table.get_key_fields(),
                                          [type(self)._update_field]))
        return {key: value for (key, value) in self.items()
                if key not in meta_fields}

    def __eq__(self, other):
        return isinstance(other, type(self)) and self.pk == other.pk

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash(tuple(self.get_keys().items()))

    def __getitem__(self, key):
        # boto's Item[key] is really Item.get(key), but this causes various
        # problems, and only makes sense for __getitem__ when undeclared fields
        # are allowed:
        if self._meta.allow_undeclared_fields:
            return self._data.get(key)

        # We provide a field property interface to do Item.get(key); there's
        # no need to lie about our underlying data:
        return self._data[key]

    @classmethod
    def _pre_set(cls, key, value, lossy=True):
        """Clean incoming data values, including exotic types (e.g. DATE)."""
        key_field = cls._meta.keys.get(key)
        if key_field:
            decoder = key_field.decode_lossy if lossy else key_field.decode
            return decoder(value)

        link_field = cls._meta.links.get(key)
        if link_field:
            raise TypeError("Access to {!r} required through descriptor"
                            .format(link_field))

        if not cls._meta.allow_undeclared_fields:
            raise TypeError("Field {!r} undeclared and unallowed by {} items"
                            .format(key, cls.__name__))

        undeclared = cls._meta.undeclared_data_type
        if isinstance(undeclared, types.DataType):
            if lossy:
                return undeclared.decode_lossy(value)
            return undeclared.decode(value)

        return value

    def _clear_link_cache(self, key):
        try:
            link_name = self._meta.link_keys[key]
        except KeyError:
            pass
        else:
            link = getattr(type(self), link_name)
            link.cache_clear(self)

    def __setitem__(self, key, value):
        value = self._pre_set(key, value)
        self._clear_link_cache(key)
        super(Item, self).__setitem__(key, value)

    def __delitem__(self, key):
        self._clear_link_cache(key)
        super(Item, self).__delitem__(key)

    # prepare_full determines data to put for save and BatchTable once they
    # know there's data to put. Insert timestamp for update:
    def prepare_full(self):
        if type(self)._update_field:
            # Always set "updated" time:
            self[type(self)._update_field] = epoch.utcnow()
        return super(Item, self).prepare_full()

    def _remove_null_values(self):
        for key, value in self.items():
            if types.is_null(value):
                del self[key]
                self._orig_data.pop(key, None)

    # partial_save's prepare_partial isn't the same sort of hook as
    # prepare_full. Perform data preparations directly:
    def partial_save(self):
        if self.needs_save():
            if type(self)._update_field:
                # Always set updated time:
                self[type(self)._update_field] = epoch.utcnow()

            # Changing a value to something NULL-y has special meaning for
            # partial_save() -- it is treated as a deletion directive.
            # We don't *think* we want this, ever; we can always delete the key
            # explicitly. So, remove NULL-y values:
            self._remove_null_values()

        return super(Item, self).partial_save()
