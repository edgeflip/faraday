from boto.dynamodb2 import fields as basefields

from . import loading, types, utils
from . import manager as managers


# FIXME: this doesn't make sense without celery
class UpsertStrategy(object):

    @staticmethod
    def overwrite(obj, key, value):
        obj[key] = value

    @staticmethod
    def combine(obj, key, value):
        try:
            obj[key] += value
        except KeyError:
            obj[key] = value

    @staticmethod
    def update(obj, key, value):
        hash_ = obj.get(key)
        if hash_:
            hash_.update(value)
        else:
            obj[key] = value


class BaseItemField(object):
    pass


class ItemField(BaseItemField):

    internal = None

    def __init__(self,
                 data_type=types.STRING,
                 upsert_strategy=UpsertStrategy.overwrite,
                 **kws):
        self.data_type = data_type
        self.upsert_strategy = upsert_strategy
        self.kws = kws

    def __repr__(self):
        dict_ = vars(self).copy()
        kws = dict_.pop('kws')
        dict_.update(kws)
        return "{}({})".format(
            type(self).__name__,
            ', '.join("{}={!r}".format(key, value) for key, value in dict_.items())
        )

    def contribute_to_class(self, item, key):
        item._meta.keys[key] = self
        setattr(item, key, FieldProperty(key))

    def make_internal(self, name):
        return self.internal and self.internal(name=name,
                                               data_type=self.data_type,
                                               **self.kws)

    def decode(self, value):
        if isinstance(self.data_type, types.DataType):
            return self.data_type.decode(value)
        return value

    def decode_lossy(self, value):
        if isinstance(self.data_type, types.DataType):
            return self.data_type.decode_lossy(value)
        return value


class HashKeyField(ItemField):

    internal = basefields.HashKey


class RangeKeyField(ItemField):

    internal = basefields.RangeKey


class ItemLinkField(BaseItemField):
    """Item field indicating a link between two Items, similar to a foreign key."""

    Unset = object()

    is_single = False

    def __init__(self, item, db_key, linked_name=Unset):
        self.item = item
        if isinstance(db_key, (tuple, list)):
            self.db_key = db_key
        else:
            self.db_key = (db_key,)
        self.linked_name = linked_name

    def __repr__(self):
        return "{}({})".format(
            self.__class__.__name__,
            ', '.join("{}={!r}".format(key, value)
                      for (key, value) in vars(self).items())
        )

    def contribute_to_class(self, item, key):
        item._meta.links[key] = self
        setattr(item, key, LinkFieldProperty(key))

        # Resolve ItemField references:
        # FIXME: inefficient to recalc each time?
        reversed_keys = {field: name for (name, field) in item._meta.keys.items()}
        self.db_key = tuple(
            reversed_keys[key_ref] if isinstance(key_ref, ItemField) else key_ref
            for key_ref in self.db_key
        )

        if item._meta.abstract:
            return

        # Construct linked item manager property
        if self.linked_name:
            if self.linked_name is self.Unset:
                # FIXME: inheritance will set bad name. perhaps store decision in _linked_name?
                self.linked_name = utils.camel_to_underscore(item._meta.item_name).replace('_', '')
                if not self.is_single:
                    if self.linked_name.endswith('s'):
                        self.linked_name += '_set'
                    else:
                        self.linked_name += 's'

            property_cls = SingleReverseLinkProperty if self.is_single else ReverseLinkManagerProperty
            reverse_link = property_cls(self.linked_name, item, self)
        else:
            reverse_link = None

        # Resolve item reference or connect listener to resolve reference once
        # the Item exists.
        linked_item = self.item
        if isinstance(linked_item, basestring):
            try:
                linked_item = loading.cache[linked_item]
            except KeyError:
                pending = loading.pending_links[linked_item]
                pending.add(self)
                if reverse_link:
                    pending.add(reverse_link)
                return

        self.resolve_link(linked_item)
        if reverse_link:
            reverse_link.resolve_link(linked_item)

    def resolve_link(self, item):
        self.item = item

    def get_item_pk(self, instance):
        return tuple(instance[key] for key in self.db_key)

    @staticmethod
    def cache_name(name):
        return '_{}_cache'.format(name)

    @classmethod
    def cache_get(cls, name, instance):
        return getattr(instance, cls.cache_name(name), None)

    @classmethod
    def cache_set(cls, name, instance, value):
        setattr(instance, cls.cache_name(name), value)

    @classmethod
    def cache_clear(cls, name, instance):
        try:
            delattr(instance, cls.cache_name(name))
        except AttributeError:
            pass


class SingleItemLinkField(ItemLinkField):

    is_single = True


# Descriptors #

class BaseFieldProperty(object):

    def __init__(self, field_name):
        self.field_name = field_name

    def __repr__(self):
        return "{}({!r})".format(self.__class__.__name__, self.field_name)


class FieldProperty(BaseFieldProperty):
    """Item field property descriptor, allowing access to the item data dictionary
    via the attribute interface.

    By applying these to the Item definition, its attribute interface may be
    preferred, and e.g. typos will raise AttributeError rather than simply returning
    None.

    """
    def __get__(self, instance, cls=None):
        return self if instance is None else instance.get(self.field_name)

    def __set__(self, instance, value):
        instance[self.field_name] = value

    def __delete__(self, instance):
        del instance[self.field_name]


class LinkFieldProperty(BaseFieldProperty):
    """Item link field descriptor, providing management of a linked Item."""

    def cache_get(self, instance):
        field = instance._meta.links[self.field_name]
        return field.cache_get(self.field_name, instance)

    def cache_set(self, instance, value):
        field = instance._meta.links[self.field_name]
        field.cache_set(self.field_name, instance, value)

    def __get__(self, instance, cls=None):
        if instance is None:
            return self

        field = instance._meta.links[self.field_name]
        result = field.cache_get(self.field_name, instance)
        if result is not None:
            return result

        try:
            manager = field.item.items
        except AttributeError:
            raise TypeError("Item link unresolved or bad link argument: {!r}"
                            .format(field.item))

        keys = manager.table.get_key_fields()
        try:
            values = field.get_item_pk(instance)
        except KeyError:
            return None

        query = dict(zip(keys, values))
        result = manager.get_item(**query)
        field.cache_set(self.field_name, instance, result)
        return result

    def __set__(self, instance, related):
        field = instance._meta.links[self.field_name]
        for (key, value) in zip(field.db_key, related.pk):
            instance[key] = value
        field.cache_set(self.field_name, instance, related)

    def __delete__(self, instance):
        field = instance._meta.links[self.field_name]
        for key in field.db_key:
            del instance[key]
        field.cache_clear(self.field_name, instance)


class AbstractReverseLinkFieldProperty(BaseFieldProperty):

    def __init__(self, field_name, item, link_field):
        super(AbstractReverseLinkFieldProperty, self).__init__(field_name)
        self.item = item
        self.link_field = link_field

    def resolve_link(self, parent):
        if hasattr(parent, self.field_name):
            raise ValueError("{} already defines attribute {}"
                             .format(parent.__name__, self.field_name))
        setattr(parent, self.field_name, self)

    @utils.cached_property
    def linked_manager_cls(self):
        # FIXME: Inheriting from a distinct BaseItemManager allows us to limit
        # FIXME: interface to only querying methods; but, this disallows inheritance
        # FIXME: of user-defined ItemManager methods...
        child_meta = self.item._meta
        link_field = self.link_field
        db_key = link_field.db_key

        class LinkedItemQuery(managers.AbstractLinkedItemQuery):

            name_child = child_meta.link_keys[db_key[0]]
            child_field = child_meta.links[name_child]

        class LinkedItemManager(managers.AbstractLinkedItemManager):

            core_keys = db_key
            query_cls = LinkedItemQuery

        return LinkedItemManager


class SingleReverseLinkProperty(AbstractReverseLinkFieldProperty):
    """The one-to-one Item link field's reversed descriptor, providing access to
    the single matching Item.

    """
    def __get__(self, instance, cls=None):
        if instance is None:
            return self

        # NOTE: If ReverseLinkFieldProperty ever supports __set__ et al, below
        # caching method won't work (it will be a data descriptor and take
        # precendence over instance __dict__). (See LinkFieldProperty.)

        default_manager = self.item.items
        db_key = self.link_field.db_key
        key_fields = set(default_manager.table.get_key_fields())
        if set(db_key) >= key_fields:
            # No need for related manager:
            query = {key: value for (key, value) in zip(db_key, instance.pk)
                     if key in key_fields}
            linked = default_manager.get_item(**query)
        else:
            manager = self.linked_manager_cls(default_manager.table, instance)
            linked = manager.filter_get()

        vars(instance)[self.field_name] = linked
        return linked


class ReverseLinkManagerProperty(AbstractReverseLinkFieldProperty):
    """The Item link field's reversed descriptor, providing access to all Items with
    matching links.

    """
    def __get__(self, instance, cls=None):
        if instance is None:
            return self

        manager = self.linked_manager_cls(self.item.items.table, instance)
        # NOTE: If ReverseLinkManagerProperty ever supports __set__ et al, below
        # caching method won't work (it will be a data descriptor and take
        # precendence over instance __dict__). (See LinkFieldProperty.)
        vars(instance)[self.field_name] = manager
        return manager
