from nose import tools

from faraday import HashKeyField, Item, ItemField, NUMBER, STRING_SET

from . import FaradayTestCase


class TestStringSetCSV(FaradayTestCase):

    @classmethod
    def setup_class(cls):
        class User(Item):
            id = HashKeyField(data_type=NUMBER)
            books = ItemField(data_type=STRING_SET)

        cls.User = User

    def test_garbage(self):
        garbage = u'"'
        item = self.User(id=1, books=garbage, loaded=True)
        tools.eq_(item.id, 1)
        tools.eq_(item.books, {garbage})
