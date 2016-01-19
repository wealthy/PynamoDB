"""
An example using Amazon's Thread example for motivation

http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/SampleTablesAndData.html
"""
from __future__ import print_function
import logging
from pynamodb.models import Model
from pynamodb.attributes import (
    UnicodeAttribute, NumberAttribute, UnicodeSetAttribute, UTCDateTimeAttribute, MapAttribute, ListAttribute, BooleanAttribute
)
from datetime import datetime
import pdb

logging.basicConfig()
log = logging.getLogger("pynamodb")
log.setLevel(logging.DEBUG)
log.propagate = True


class Thread(Model):
    class Meta:
        read_capacity_units = 1
        write_capacity_units = 1
        table_name = "Thread"
        host = "http://localhost:8000"
    forum_name = UnicodeAttribute(hash_key=True)
    subject = UnicodeAttribute(range_key=True)
    views = NumberAttribute(default=0)
    replies = NumberAttribute(default=0)
    answered = NumberAttribute(default=0)
    tags = UnicodeSetAttribute()
    last_post_datetime = UTCDateTimeAttribute(null=True)
    test_map = MapAttribute(null=True)
    test_list = ListAttribute(null=True)
    public = BooleanAttribute(null=True)

# Create the table
if not Thread.exists():
    Thread.create_table(wait=True)

# Create a thread
thread_item = Thread(
    'Test_map',
    'subject 3',
    tags=['foo', 'bar'],
    last_post_datetime=datetime.now(),
    test_map={'k1' : 'v1', 'k2' : True, 'k3' : {'k3k1' : 'k3v1'}, 'k4' : ['1', 2]},
    test_list=['12', '13', 14],
    public=True
)
thread_item.save()

thread_item = Thread(
    'Test_map',
    'subject 4',
    tags=['foo', 'bar'],
    last_post_datetime=datetime.now(),
    test_map={'k1' : 'v1', 'k2' : False, 'k3' : {'k3k1' : 'k3v1'}, 'k4' : ['1', 2]},
    test_list=['12', '13', 14],
    public=False
)
thread_item.save()

for item in Thread.scan():
    print(item)
    print(item.test_map)
    print(item.test_list)
    print(item.public)