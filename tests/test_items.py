from nose import tools

import faraday

from . import FaradayTestCase


class TestEquality(FaradayTestCase):

    class Token(faraday.Item):
        uid = faraday.HashKeyField(data_type=faraday.NUMBER)
        token = faraday.RangeKeyField()

        class Meta(object):
            app_name = 'faraday'

    def setup(self):
        super(TestEquality, self).setup()
        self.create_item_table(self.Token)

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

    def setup(self):
        super(TestLinkedItems, self).setup()

        class User(faraday.Item):
            uid = faraday.HashKeyField(data_type=faraday.NUMBER)

        class Token(faraday.Item):
            uid = faraday.HashKeyField(data_type=faraday.NUMBER)
            token = faraday.RangeKeyField()
            user = faraday.ItemLinkField(User, db_key=uid)

        self.User = User
        self.Token = Token

    def test_init(self):
        user = self.User(uid=123)
        token = self.Token(user=user, token='abc')
        tools.eq_(dict(token), {'uid': 123, 'token': 'abc'})
        tools.eq_(vars(token)['_user_cache'], user)
