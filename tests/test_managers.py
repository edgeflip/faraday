from mock import patch
from nose import tools

import faraday

from . import FaradayTestCase


class TestPrefetch(FaradayTestCase):

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

    def setup(self):
        super(TestPrefetch, self).setup()

        self.user = self.User(uid=123)
        self.user.save()
        self.tokens = [self.Token(token=token_token) for token_token in ('abc', 'xyz')]
        for token in self.tokens:
            token.user = self.user
            token.save()

    def test_prefetch_parent_link(self):
        for token in self.user.tokens.all():
            tools.assert_true(vars(token).get('_user_cache'))
            get_item = self.User.items.table.get_item
            with patch.object(self.User.items.table, 'get_item') as mock_get_item:
                mock_get_item.side_effect = get_item
                user = token.user
            tools.assert_false(mock_get_item.called)
            tools.eq_(user, self.user)

    def test_prefetch_named_linked(self):
        for token in self.Token.items.prefetch('user').scan():
            tools.assert_true(vars(token).get('_user_cache'))
            get_item = self.User.items.table.get_item
            with patch.object(self.User.items.table, 'get_item') as mock_get_item:
                mock_get_item.side_effect = get_item
                user = token.user
            tools.assert_false(mock_get_item.called)
            tools.eq_(user, self.user)
