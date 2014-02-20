from nose import tools

from faraday import utils


class TestCachedProperty(object):

    @classmethod
    def setup_class(cls):
        class CachedPropertyObject(object):

            def __init__(self):
                self.call_count = 0

            @utils.cached_property
            def method(self):
                self.call_count += 1
                return 55

        setattr(cls, CachedPropertyObject.__name__, CachedPropertyObject)

    def test_caching(self):
        obj = self.CachedPropertyObject()
        tools.eq_(vars(obj), {'call_count': 0})
        tools.eq_(obj.method, 55)
        tools.eq_(obj.method, 55)
        tools.eq_(vars(obj), {'call_count': 1, 'method': 55})

    def test_class_access(self):
        tools.assert_is_instance(self.CachedPropertyObject.method,
                                 utils.cached_property)


class TestClassProperty(object):

    @classmethod
    def setup_class(cls):
        class ClassPropertyObject(object):

            _value = 55

            @utils.class_property
            def method(kls):
                return kls._value

        setattr(cls, ClassPropertyObject.__name__, ClassPropertyObject)

    def test_class(self):
        tools.eq_(self.ClassPropertyObject.method, 55)

    def test_object(self):
        tools.eq_(self.ClassPropertyObject().method, 55)
