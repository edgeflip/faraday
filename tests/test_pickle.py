import pickle

from nose import tools

import faraday

from . import FaradayTestCase


class User(faraday.Item):
    uid = faraday.HashKeyField(data_type=faraday.NUMBER)

    class Meta(object):
        app_name = 'pickling'


class Token(faraday.Item):
    uid = faraday.HashKeyField(data_type=faraday.NUMBER)
    token = faraday.RangeKeyField()
    user = faraday.ItemLinkField(User, db_key=uid)

    class Meta(object):
        app_name = 'pickling'


class TestPickling(FaradayTestCase):

    def test_pickle(self):
        pickle.dumps(Token(uid=123, token='abc'), 2)

    def test_pickle_linked_manager(self):
        user = User(uid=123)
        tools.assert_true(user.tokens)
        pickle.dumps(user, 2)
