import unittest
from datetime import datetime

import pytz

from ckanext.mongodatastore.mongodb_controller import convert_to_csv, convert_to_unix_timestamp, MongoDbController

TEST_RESULT_SET = [
    {'id': 1, 'name': 'Florian', 'age': 12},
    {'id': 2, 'name': 'Michael', 'age': 13}
]

TEST_RESULT_SET_KEYS = ['id', 'name', 'age']
TEST_RESULT_CSV = "1;Florian;12\r\n2;Michael;13\r\n"


class MongoDbControllerTest(unittest.TestCase):
    def test_convert_to_csv(self):
        result = str(convert_to_csv(TEST_RESULT_SET, TEST_RESULT_SET_KEYS))
        self.assertEqual(result, TEST_RESULT_CSV)

    def test_convert_to_unix_timestamp(self):
        datetime_value = datetime(2019, 3, 1, 0, 0, 0, 0, tzinfo=pytz.UTC)
        unix_timestamp = convert_to_unix_timestamp(datetime_value)
        self.assertEqual(unix_timestamp, 1551398400)

    def test_convert_to_zero_timestamp(self):
        datetime_value = datetime(1970, 1, 1, 0, 0, 0, 0, tzinfo=pytz.UTC)
        unix_timestamp = convert_to_unix_timestamp(datetime_value)
        self.assertEqual(unix_timestamp, 0)
