from nose import tools

import faraday

from . import FaradayTestCase


class TestDefinition(object):

    @tools.raises(TypeError)
    def test_no_subclass(self):
        faraday.Item()


class TestAbstractInheritance(FaradayTestCase):

    @classmethod
    def setup_class(cls):
        class AbstractToken(faraday.Item):
            token = faraday.RangeKeyField()

            class Meta(object):
                abstract = True

        class UserToken(AbstractToken):
            uid = faraday.HashKeyField(data_type=faraday.NUMBER)

        class AppToken(AbstractToken):
            appid = faraday.HashKeyField(data_type=faraday.NUMBER)

        cls.AbstractToken = AbstractToken
        cls.UserToken = UserToken
        cls.AppToken = AppToken

    @tools.raises(TypeError)
    def test_abstract_item(self):
        self.AbstractToken()

    @tools.raises(AttributeError)
    def test_abstract_manager(self):
        self.AbstractToken.items

    def test_abstract_meta(self):
        meta = self.AbstractToken._meta
        tools.assert_true(meta.abstract)
        tools.assert_is_instance(meta.keys['token'], faraday.RangeKeyField)
        tools.assert_is_none(meta.table_name)

    def test_inheritance(self):
        tools.assert_false(self.UserToken._meta.abstract)

        tools.eq_(sorted(self.UserToken._meta.fields.keys()),
                  ['token', 'uid', 'updated'])
        tools.eq_(sorted(self.AppToken._meta.fields.keys()),
                  ['appid', 'token', 'updated'])

        tools.assert_is_not(self.UserToken._meta.fields['token'],
                            self.AbstractToken._meta.fields['token'])

        user_token = self.UserToken(uid=123, token='abc')
        tools.eq_(sorted(user_token.items()), [('token', 'abc'), ('uid', 123)])


class TestEquality(FaradayTestCase):

    @classmethod
    def setup_class(cls):
        class Token(faraday.Item):
            uid = faraday.HashKeyField(data_type=faraday.NUMBER)
            token = faraday.RangeKeyField()

        cls.Token = Token

    def test_eq(self):
        t0 = self.Token(uid=123, token='abc')
        t1 = self.Token(uid=123, token='abc')
        tools.assert_is_not(t0, t1)
        tools.assert_equal(t0, t1)
        tools.assert_equal(hash(t0), hash(t1))

    def test_ne(self):
        t0 = self.Token(uid=923, token='abc')
        t1 = self.Token(uid=123, token='abc')
        tools.assert_not_equal(t0, t1)
        tools.assert_not_equal(hash(t0), hash(t1))


class TestLinkedItems(FaradayTestCase):

    @classmethod
    def setup_class(cls):
        class User(faraday.Item):
            uid = faraday.HashKeyField(data_type=faraday.NUMBER)

        class Token(faraday.Item):
            uid = faraday.HashKeyField(data_type=faraday.NUMBER)
            token = faraday.RangeKeyField()
            user = faraday.ItemLinkField(User, db_key=uid)

        cls.User = User
        cls.Token = Token

    def test_init(self):
        user = self.User(uid=123)
        token = self.Token(user=user, token='abc')
        tools.eq_(dict(token), {'uid': 123, 'token': 'abc'})
        tools.eq_(vars(token)['_user_cache'], user)

    def test_retrieval(self):
        user = self.User.items.create(uid=123)
        token = self.Token(uid=123, token='abc')
        tools.assert_not_in('_user_cache', vars(token))
        tools.eq_(dict(token), {'uid': 123, 'token': 'abc'})
        tools.eq_(token.user, user)
        tools.assert_is_not(token.user, user)
        tools.assert_in('_user_cache', vars(token))
