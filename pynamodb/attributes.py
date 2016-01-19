"""
PynamoDB attributes
"""
import six
import json
from base64 import b64encode, b64decode
from delorean import Delorean, parse
from pynamodb.constants import (
    STRING, NUMBER, BOOLEAN, BINARY, UTC, DATETIME_FORMAT, BINARY_SET, STRING_SET, NUMBER_SET,
    MAP, LIST, DEFAULT_ENCODING, ATTR_TYPE_MAP
)

class BaseAttribute(object):
    """
    Base class for all attributes. This should not be extended, instead one of the attribute 
    classes should be extended, instantiated.
    """
    attr_type = None
    null = True

    def __init__(self, 
                null=None, 
                default=None
                ):
        if null is not None:
            self.null = null
        self.default = default

    def __set__(self, instance, value):
        if isinstance(value, Attribute):
            return self
        if instance:
            instance.attribute_values[self.attr_name] = value

    def __get__(self, instance, owner):
        if instance:
            return instance.attribute_values.get(self.attr_name, None)
        else:
            return self

    def serialize(self, value):
        """
        This method should return a dynamodb compatible value
        """
        return value

    def deserialize(self, value):
        """
        Performs any needed deserialization on the value
        """
        return value


class Attribute(BaseAttribute):
    """
    An attribute of a model
    """
    null = False
    attr_name = None

    def __init__(self,
                 hash_key=False,
                 range_key=False,
                 null=None,
                 default=None,
                 attr_name=None
                 ):
        self.is_hash_key = hash_key
        self.is_range_key = range_key
        if attr_name is not None:
            self.attr_name = attr_name
        super(Attribute, self).__init__(
                                    null=null, 
                                    default=default
                                    )


class SetMixin(object):
    """
    Adds (de)serialization methods for sets
    """
    def serialize(self, value):
        """
        Serializes a set

        Because dynamodb doesn't store empty attributes,
        empty sets return None
        """
        if value is not None:
            try:
                iter(value)
            except TypeError:
                value = [value]
            if len(value):
                return [json.dumps(val) for val in sorted(value)]
        return None

    def deserialize(self, value):
        """
        Deserializes a set
        """
        if value and len(value):
            return set([json.loads(val) for val in value])


class BaseBinaryAttribute(BaseAttribute):
    """
    Base class for binary attributes. This is used to serialize/deserialize nested attributes
    in maps and lists

    This class should not be instantiated outside this module.
    """
    attr_type = BINARY

    def serialize(self, value):
        """
        Returns a base64 encoded binary string
        """
        return b64encode(value).decode(DEFAULT_ENCODING)

    def deserialize(self, value):
        """
        Returns a decoded string from base64
        """
        try:
            return b64decode(value.decode(DEFAULT_ENCODING))
        except AttributeError:
            return b64decode(value)


class BinaryAttribute(Attribute, BaseBinaryAttribute):
    """
    A binary attribute
    """
    pass


class BinarySetAttribute(SetMixin, Attribute):
    """
    A binary set
    """
    attr_type = BINARY_SET
    null = True

    def serialize(self, value):
        """
        Returns a base64 encoded binary string
        """
        if value and len(value):
            return [b64encode(val).decode(DEFAULT_ENCODING) for val in sorted(value)]
        else:
            return None

    def deserialize(self, value):
        """
        Returns a decoded string from base64
        """
        if value and len(value):
            return set([b64decode(val.encode(DEFAULT_ENCODING)) for val in value])


class UnicodeSetAttribute(SetMixin, Attribute):
    """
    A unicode set
    """
    attr_type = STRING_SET
    null = True


class BaseUnicodeAttribute(BaseAttribute):
    """
    Base class for unicode attributes. This is used to serialize/deserialize nested attributes
    in maps and lists

    This class should not be instantiated outside this module.
    """
    attr_type = STRING

    def serialize(self, value):
        """
        Returns a unicode string
        """
        if value is None or not len(value):
            return None
        elif isinstance(value, six.text_type):
            return value
        else:
            return six.u(value)


class UnicodeAttribute(Attribute, BaseUnicodeAttribute):
    """
    A unicode attribute
    """
    pass


class JSONAttribute(Attribute):
    """
    A JSON Attribute

    Encodes JSON to unicode internally
    """
    attr_type = STRING

    def serialize(self, value):
        """
        Serializes JSON to unicode
        """
        if value is None:
            return None
        encoded = json.dumps(value)
        try:
            return unicode(encoded)
        except NameError:
            return encoded

    def deserialize(self, value):
        """
        Deserializes JSON
        """
        return json.loads(value, strict=False)


class BaseBooleanAttribute(BaseAttribute):
    """
    Base class for boolean attributes. This is used to serialize/deserialize nested attributes
    in maps and lists

    This class should not be instantiated outside this module.
    """
    attr_type = BOOLEAN


class BooleanAttribute(Attribute, BaseBooleanAttribute):
    """
    A class for boolean attributes
    This attribute type uses a number attribute to save space
    """
    pass


class NumberSetAttribute(SetMixin, Attribute):
    """
    A number set attribute
    """
    attr_type = NUMBER_SET
    null = True


class BaseNumberAttribute(BaseAttribute):
    """
    Base class for number attributes. This is used to serialize/deserialize nested attributes
    in maps and lists

    This class should not be instantiated outside this module.
    """
    attr_type = NUMBER

    def serialize(self, value):
        """
        Encode numbers as JSON
        """
        return json.dumps(value)

    def deserialize(self, value):
        """
        Decode numbers from JSON
        """
        return json.loads(value)


class NumberAttribute(Attribute, BaseNumberAttribute):
    """
    A number attribute
    """
    pass


class UTCDateTimeAttribute(Attribute):
    """
    An attribute for storing a UTC Datetime
    """
    attr_type = STRING

    def serialize(self, value):
        """
        Takes a datetime object and returns a string
        """
        fmt = Delorean(value, timezone=UTC).datetime.strftime(DATETIME_FORMAT)
        return six.u(fmt)

    def deserialize(self, value):
        """
        Takes a UTC datetime string and returns a datetime object
        """
        return parse(value).datetime


class BaseMapAttribute(BaseAttribute):
    """
    Base class for map attributes. This is used to serialize/deserialize nested attributes
    in maps and lists

    This class should not be instantiated outside this module.
    """
    attr_type = MAP

    def serialize(self, dictionary):
        """
        """
        attrs = {}
        if not isinstance(dictionary, dict):
            raise TypeError("Only map(dict) types can be serialized using this method. Use the " + 
                "other pynamodb types if a map is not being used.")
        for key, value in dictionary.iteritems():
            if not isinstance(key, basestring):
                raise TypeError("Map keys must be strings.")
            value_type = get_pynamo_type(value)
            serialized = value_type.serialize(value)
            attrs[key] = {
                ATTR_TYPE_MAP[value_type.attr_type] : serialized
            }
        return attrs

    def deserialize(self, value):
        """
        """
        data = {}
        for key, item in value.iteritems():
            data[key] = get_python_type(item)
        return data


class MapAttribute(Attribute, BaseMapAttribute):
    """
    A Map attribute. Makes use of the dynamodb document data types.
    """
    pass


class BaseListAttribute(BaseAttribute):
    """
    Base class for list attributes. This is used to serialize/deserialize nested attributes
    in maps and lists

    This class should not be instantiated outside this module.
    """
    attr_type = LIST

    def serialize(self, values):
        """
        """
        attrs = []
        if not isinstance(values, list):
            raise TypeError("Only list types can be serialized using this method." + 
                " Use other pynamodb types if a list is not being used.")
        for value in values:
            value_type = get_pynamo_type(value)
            serialized = value_type.serialize(value)
            attrs.append({
                ATTR_TYPE_MAP[value_type.attr_type] : serialized
            })
        return attrs

    def deserialize(self, value):
        """
        """
        data = []
        for item in value:
            data.append(get_python_type(item))
        return data


class ListAttribute(Attribute, BaseListAttribute):
    """
    A List attribute. Makes use of the dynamodb document data types.
    """
    pass


STRING_TYPE = BaseUnicodeAttribute()
NUMBER_TYPE = BaseNumberAttribute()
BOOLEAN_TYPE = BaseBooleanAttribute()
# BINARY_TYPE = BaseBinaryAttribute()
MAP_TYPE = BaseMapAttribute()
LIST_TYPE = BaseListAttribute()

#TODO@rohan - switch to using the boolean type in Pynamo and storing it as true or false. 
def get_pynamo_type(value):
    if isinstance(value, basestring):
        return STRING_TYPE
    elif isinstance(value, (int, float)):
        return NUMBER_TYPE
    elif isinstance(value, dict):
        return MAP_TYPE
    elif isinstance(value, list):
        return LIST_TYPE
    raise TypeError("Trying to use a python type that is not supported. Only, string, int, float, dict and list are supported.")

def get_python_type(dynamo_value):
    if not isinstance(dynamo_value, dict):
        raise TypeError("The dict object returned by dynamodb should be parsed for getting the python native type.")
    value_type = dynamo_value.keys()[0]
    if ATTR_TYPE_MAP[value_type] == STRING:
        return STRING_TYPE.deserialize(dynamo_value[value_type])
    elif ATTR_TYPE_MAP[value_type] == NUMBER:
        return NUMBER_TYPE.deserialize(dynamo_value[value_type])
    elif ATTR_TYPE_MAP[value_type] == MAP:
        return MAP_TYPE.deserialize(dynamo_value[value_type])
    elif ATTR_TYPE_MAP[value_type] == LIST:
        return LIST_TYPE.deserialize(dynamo_value[value_type])
    raise TypeError("The given Dynamodb type is not supported. Type: " + value_type)