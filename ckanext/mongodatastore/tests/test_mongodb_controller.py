import unittest
from datetime import datetime

import pytz

from ckanext.mongodatastore.helper import CKAN_DATASTORE
from ckanext.mongodatastore.mongodb_controller import convert_to_csv, convert_to_unix_timestamp, MongoDbController

from time import sleep

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
        self.assertNotEqual(col.count_documents({}), 0)

        mongo_cntr.delete_resource(new_resource_id, None)

        self.assertEqual(col.count_documents({}), 2)

        self.assertTrue(mongo_cntr.resource_exists(new_resource_id))

        mongo_cntr.delete_resource(new_resource_id, None, force=True)

        self.assertFalse(mongo_cntr.resource_exists(new_resource_id))

    def test_update_datatypes(self):
        mongo_cntr = MongoDbController.getInstance()

        new_resource_id = 'new_resource'
        primary_key = 'id'

        new_records = [
            {'id': 1, 'field1': '123', 'field2': 123},
            {'id': 2, 'field1': '456', 'field2': 456}
        ]

        mongo_cntr.create_resource(new_resource_id, primary_key)

        self.assertTrue(mongo_cntr.resource_exists(new_resource_id))

        mongo_cntr.upsert(new_resource_id, new_records, False)

        sleep(1)

        fields = mongo_cntr.resource_fields(new_resource_id)

        self.assertEqual(fields['schema'].keys(), ['field1', 'field2', 'id'])
        self.assertEqual(fields['schema']['field1'], 'string')
        self.assertEqual(fields['schema']['field2'], 'number')
        self.assertEqual(fields['schema']['id'], 'number')

        mongo_cntr.update_datatypes(new_resource_id, [{'id': 'field1', 'info': {'type_override': 'number'}}])

        fields = mongo_cntr.resource_fields(new_resource_id)

        self.assertEqual(fields['schema'].keys(), ['field1', 'field2', 'id'])
        self.assertEqual(fields['schema']['field1'], 'number')
        self.assertEqual(fields['schema']['field2'], 'number')
        self.assertEqual(fields['schema']['id'], 'number')

    def test_upsert(self):
        mongo_cntr = MongoDbController.getInstance()

        new_resource_id = 'new_resource'
        primary_key = 'id'

        new_records = [
            {'id': 1, 'field1': 'abc', 'field2': 123},
            {'id': 2, 'field1': 'def', 'field2': 456}
        ]

        updated_records = [
            {'id': 2, 'field1': 'new_value', 'field2': 1},
            {'id': 3, 'field1': 'ghi', 'field2': 1}
        ]

        mongo_cntr.create_resource(new_resource_id, primary_key)

        self.assertTrue(mongo_cntr.resource_exists(new_resource_id))

        mongo_cntr.upsert(new_resource_id, new_records, False)

        sleep(1)

        result = mongo_cntr.query_current_state(new_resource_id, {}, {'_id': 0, 'id': 1, 'field1': 1, 'field2': 1},
                                                None, None, None, None, False)

        mongo_cntr.upsert(new_resource_id, updated_records, False)

        sleep(1)

        updated_result = mongo_cntr.query_current_state(new_resource_id, {},
                                                        {'_id': 0, 'id': 1, 'field1': 1, 'field2': 1},
                                                        None, None, None, None, False)

        self.assertEqual(len(result['records']), 2)
        self.assertEqual(len(updated_result['records']), 3)

        self.assertEqual(result['records'][0], {'id': 1, 'field1': 'abc', 'field2': 123})
        self.assertEqual(result['records'][1], {'id': 2, 'field1': 'def', 'field2': 456})

        self.assertEqual(updated_result['records'][0], {'id': 1, 'field1': 'abc', 'field2': 123})
        self.assertEqual(updated_result['records'][1], {'id': 2, 'field1': 'new_value', 'field2': 1})
        self.assertEqual(updated_result['records'][2], {'id': 3, 'field1': 'ghi', 'field2': 1})

    def test_retrieve_stored_query(self):
        mongo_cntr = MongoDbController.getInstance()

        new_resource_id = 'new_resource'
        primary_key = 'id'

        new_records = [
            {u'id': 1, u'field1': u'abc', u'field2': 123},
            {u'id': 2, u'field1': u'def', u'field2': 456},
            {u'id': 3, u'field1': u'ghi', u'field2': 456}
        ]

        updated_records = [
            {u'id': 3, u'field1': u'new_value', u'field2': 321},
            {u'id': 4, u'field1': u'jkl', u'field2': 432}
        ]

        mongo_cntr.create_resource(new_resource_id, primary_key)

        self.assertTrue(mongo_cntr.resource_exists(new_resource_id))

        mongo_cntr.upsert(new_resource_id, new_records, False)

        sleep(1)

        result = mongo_cntr.query_current_state(new_resource_id, {}, {u'_id': 0, u'id': 1, u'field1': 1, u'field2': 1},
                                                None, None, None, None, False)

        mongo_cntr.upsert(new_resource_id, updated_records, False)

        sleep(1)

        mongo_cntr.delete_resource(new_resource_id, {u'id': 1})

        sleep(1)

        new_result = mongo_cntr.query_current_state(new_resource_id, {},
                                                    {u'_id': 0, u'id': 1, u'field1': 1, u'field2': 1},
                                                    None, None, None, None, False)

        history_result = mongo_cntr.retrieve_stored_query(result[u'pid'], None, None, False)

        print(result['pid'])
        print(result['records'])
        print(history_result['records'])

        self.assertTrue(False)

        self.assertEqual(result[u'records'], [{u'id': 1, u'field1': u'abc', u'field2': 123},
                                              {u'id': 2, u'field1': u'def', u'field2': 456},
                                              {u'id': 3, u'field1': u'ghi', u'field2': 456}])

        self.assertEqual(new_result[u'records'], [{u'id': 2, u'field1': u'def', u'field2': 456},
                                                  {u'id': 3, u'field1': u'new_value', u'field2': 321},
                                                  {u'id': 4, u'field1': u'jkl', u'field2': 432}])

        self.assertEqual(history_result[u'records'], [{u'id': 1, u'field1': u'abc', u'field2': 123},
                                                      {u'id': 2, u'field1': u'def', u'field2': 456},
                                                      {u'id': 3, u'field1': u'ghi', u'field2': 456}])
