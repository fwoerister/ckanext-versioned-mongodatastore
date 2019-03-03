import unittest
from datetime import datetime

import pytz

from ckanext.mongodatastore.helper import CKAN_DATASTORE
from ckanext.mongodatastore.mongodb_controller import convert_to_csv, convert_to_unix_timestamp, MongoDbController

TEST_RESULT_SET = [
    {'id': 1, 'name': 'Florian', 'age': 12},
    {'id': 2, 'name': 'Michael', 'age': 13}
]

TEST_RESULT_SET_KEYS = ['id', 'name', 'age']
TEST_RESULT_CSV = "1;Florian;12\r\n2;Michael;13\r\n"


class MongoDbControllerTest(unittest.TestCase):

    def setUp(self):
        instance = MongoDbController.getInstance()
        instance.client.drop_database(CKAN_DATASTORE)
        instance.datastore = instance.client.get_database(CKAN_DATASTORE)

    # helper function tests

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

    # static function tests

    def test_singleton(self):
        first_instance = MongoDbController.getInstance()
        second_instance = MongoDbController.getInstance()

        self.assertEqual(first_instance, second_instance)

    def test_reaload_config(self):
        new_cfg = {
            'ckan.datastore.write_url': 'localhost:27017',
            'ckan.querystore.url': 'postgresql://query_store:query_store@localhost/query_store',
            'ckan.datastore.search.rows_max': 120

        }

        first_instance = MongoDbController.getInstance()
        MongoDbController.reloadConfig(new_cfg)
        second_instance = MongoDbController.getInstance()

        self.assertNotEqual(first_instance, second_instance)

    # member function tests

    def test_create_resource(self):
        mongo_cntr = MongoDbController.getInstance()

        new_resource_id = 'new_resource'
        primary_key = 'id'

        mongo_cntr.create_resource(new_resource_id, primary_key)
        all_ids = mongo_cntr.get_all_ids()

        self.assertIn(new_resource_id, all_ids)

        self.assertIn('{0}_meta'.format(new_resource_id), mongo_cntr.datastore.list_collection_names())

    def test_empty_resource(self):
        mongo_cntr = MongoDbController.getInstance()

        new_resource_id = 'new_resource'
        primary_key = 'id'

        mongo_cntr.create_resource(new_resource_id, primary_key)

        self.assertTrue(mongo_cntr.resource_exists(new_resource_id))

        mongo_cntr.delete_resource(new_resource_id, None)

        self.assertTrue(mongo_cntr.resource_exists(new_resource_id))

        mongo_cntr.delete_resource(new_resource_id, None, force=True)

        self.assertFalse(mongo_cntr.resource_exists(new_resource_id))

    def test_resource_with_records(self):
        mongo_cntr = MongoDbController.getInstance()

        new_resource_id = 'new_resource'
        primary_key = 'id'

        new_records = [
            {'id': 1, 'field1': 'abc', 'field2': 123},
            {'id': 2, 'field1': 'def', 'field2': 456}
        ]

        mongo_cntr.create_resource(new_resource_id, primary_key)

        self.assertTrue(mongo_cntr.resource_exists(new_resource_id))

        mongo_cntr.upsert(new_resource_id, new_records, False)

        col = mongo_cntr.datastore.get_collection(new_resource_id)
        self.assertNotEqual(col.count_documents(), 0)

        mongo_cntr.delete_resource(new_resource_id, None)

        self.assertEqual(col.count_documents(), 0)

        self.assertTrue(mongo_cntr.resource_exists(new_resource_id))

        mongo_cntr.delete_resource(new_resource_id, None, force=True)

        self.assertFalse(mongo_cntr.resource_exists(new_resource_id))
