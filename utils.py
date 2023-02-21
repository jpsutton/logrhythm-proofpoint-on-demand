import gc
import sys
import json
import time
import datetime
from dictor import dictor


time_format = '%Y-%m-%dT%H:%M:%S.%f%z'


def join_recipients(recipient_list):
    if not hasattr(recipient_list, '__iter__'):
        return recipient_list

    return ";".join(recipient_list)


def bytes2kilobytes(b):
    return int(b) / 1000


def get_field(field_map, record, key):
    # Parse the base value
    sep = field_map[key]['sep'] if 'sep' in field_map[key] else '.'
    value = dictor(record, field_map[key]['field'], pathsep=sep)

    # If a filter function is defined, run the raw value through the filter
    if 'filter' in field_map[key]:
        func = field_map[key]['filter']
        value = func(value)

    return value


def normalize_time(ts_str):
    # Convert a string timestamp to a Unix timestamp
    ts = datetime.datetime.strptime(ts_str, time_format)
    return int(time.mktime(ts.timetuple()))


def size_of_obj(d):
    return len(json.dumps(d))
