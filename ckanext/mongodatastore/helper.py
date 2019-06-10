import hashlib
from collections import OrderedDict
from json import JSONEncoder


HASH_ALGORITHM = hashlib.md5


def normalize_json(json_data, max_depth=3):
    ordered_dict = OrderedDict()

    _normalize_json(json_data, ordered_dict, max_depth)

    return ordered_dict


def _normalize_json(obj, target, max_depth, id_on_first_pos=False):
    if id_on_first_pos:
        if 'id' in obj.keys():
            target['id'] = obj['id']
            obj.pop('id', None)

    for key in sorted(obj.keys()):
        if type(obj[key]) is dict:
            target[key] = OrderedDict()
            _normalize_json(obj[key], target[key], max_depth - 1)
        else:
            target[key] = obj[key]


def calculate_hash(data):
    encoder = JSONEncoder()
    algo = HASH_ALGORITHM()

    if type(data) == str:
        algo.update(data)
    else:
        for doc in data:
            algo.update(encoder.encode(doc).encode('utf-8'))

    return algo.hexdigest()
