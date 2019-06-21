import copy
import logging
import unittest

from ckan.common import config

from ckanext.mongodatastore.mongodb_controller import convert_to_csv, MongoDbController, \
    MongoDbControllerException, QueryNotFoundException

log = logging.getLogger(__name__)

CKAN_DATASTORE = config.get(u'ckan.datastore.database')

class MongoDbControllerTest(unittest.TestCase):

    def setUp(self):
        instance = MongoDbController.getInstance()
        instance.client.drop_database(CKAN_DATASTORE)
        instance.datastore = instance.client.get_database(CKAN_DATASTORE)

        self.PRIMARY_KEY = 'id'
        self.RESOURCE_ID = 'test_resource'

        self.FIELDS = [
            {'id': 'distinct_field', 'type': 'number', 'info': None},
            {'id': 'field1', 'type': 'text', 'info': None},
            {'id': 'field2', 'type': 'number', 'info': None},
            {'id': 'id', 'type': 'number', 'info': None}
        ]

        self.DATA_RECORD = [
            {u'id': 1, u'field1': u'abc', u'field2': 123, 'distinct_field': 1},
            {u'id': 2, u'field1': u'def', u'field2': 456, 'distinct_field': 1},
            {u'id': 3, u'field1': u'ghi', u'field2': 456, 'distinct_field': 1}
        ]

        self.DATA_RECORD_KEYS = ['distinct_field', 'field1', 'field2', 'id']
        self.DATA_RECORD_CSV = "1;abc;123;1\r\n1;def;456;2\r\n1;ghi;456;3\r\n"

        self.DATA_RECORD_UPDATE = [
            {u'id': 1, u'field1': u'abc', u'field2': 123, 'distinct_field': 1},
            {u'id': 3, u'field1': u'new_value', u'field2': 321, 'distinct_field': 1},
            {u'id': 4, u'field1': u'jkl', u'field2': 432, 'distinct_field': 2}
        ]

        self.DATA_RECORD_INVALID_UPDATE = [
            {u'field1': u'abc', u'field2': u'abc'}
        ]

        instance.querystore.purge_query_store()

    def tearDown(self):
        instance = MongoDbController.getInstance()
        instance.client.drop_database(CKAN_DATASTORE)
        instance.datastore = instance.client.get_database(CKAN_DATASTORE)

    # helper function tests

    def test_convert_to_csv(self):
        result = str(convert_to_csv(copy.deepcopy(self.DATA_RECORD), self.DATA_RECORD_KEYS))
        self.assertEqual(result, self.DATA_RECORD_CSV)

    # static function tests

    def test_singleton(self):
        first_instance = MongoDbController.getInstance()
        second_instance = MongoDbController.getInstance()

        self.assertEqual(first_instance, second_instance)

    def test_reaload_config(self):
        new_cfg = {
            'ckan.datastore.write_url': 'localhost:27017',
            'ckan.querystore.url': 'postgresql://test_query_store:test_query_store@localhost/test_query_store',
            'ckan.datastore.search.rows_max': 120
        }

        first_instance = MongoDbController.getInstance()
        MongoDbController.reloadConfig(new_cfg)
        second_instance = MongoDbController.getInstance()

        self.assertNotEqual(first_instance, second_instance)

    # member function tests

    def test_create_resource(self):
        mongo_cntr = MongoDbController.getInstance()

        mongo_cntr.create_resource(self.RESOURCE_ID, self.PRIMARY_KEY)
        mongo_cntr.update_schema(self.RESOURCE_ID, self.FIELDS)
        all_ids = mongo_cntr.get_all_ids()

        self.assertIn(self.RESOURCE_ID, all_ids)
        self.assertIn('{0}_meta'.format(self.RESOURCE_ID), mongo_cntr.datastore.list_collection_names())

    def test_empty_resource(self):
        mongo_cntr = MongoDbController.getInstance()

        mongo_cntr.create_resource(self.RESOURCE_ID, self.PRIMARY_KEY)
        mongo_cntr.update_schema(self.RESOURCE_ID, self.FIELDS)

        self.assertTrue(mongo_cntr.resource_exists(self.RESOURCE_ID))

        mongo_cntr.delete_resource(self.RESOURCE_ID, None)

        self.assertTrue(mongo_cntr.resource_exists(self.RESOURCE_ID))

        mongo_cntr.delete_resource(self.RESOURCE_ID, None, force=True)

        self.assertFalse(mongo_cntr.resource_exists(self.RESOURCE_ID))

    def test_resource_with_records(self):
        mongo_cntr = MongoDbController.getInstance()

        mongo_cntr.create_resource(self.RESOURCE_ID, self.PRIMARY_KEY)
        mongo_cntr.update_schema(self.RESOURCE_ID, self.FIELDS)

        self.assertTrue(mongo_cntr.resource_exists(self.RESOURCE_ID))

        mongo_cntr.upsert(self.RESOURCE_ID, copy.deepcopy(self.DATA_RECORD), False)

        col = mongo_cntr.datastore.get_collection(self.RESOURCE_ID)
        self.assertNotEqual(col.count_documents({}), 0)

        mongo_cntr.delete_resource(self.RESOURCE_ID, None)

        self.assertEqual(col.count_documents({}), 6)

        self.assertTrue(mongo_cntr.resource_exists(self.RESOURCE_ID))

        mongo_cntr.delete_resource(self.RESOURCE_ID, None, force=True)

        self.assertFalse(mongo_cntr.resource_exists(self.RESOURCE_ID))

    def test_update_datatypes(self):
        mongo_cntr = MongoDbController.getInstance()

        mongo_cntr.create_resource(self.RESOURCE_ID, self.PRIMARY_KEY)
        mongo_cntr.update_schema(self.RESOURCE_ID, self.FIELDS)

        for f in self.FIELDS:
            if '_id' in f.keys():
                f.pop('_id')

        self.assertTrue(mongo_cntr.resource_exists(self.RESOURCE_ID))

        mongo_cntr.upsert(self.RESOURCE_ID, copy.deepcopy(self.DATA_RECORD), False)

        fields = mongo_cntr.resource_fields(self.RESOURCE_ID)

        self.assertEqual(fields['schema'], self.FIELDS)

    def test_update_datatypes_with_value_error(self):
        mongo_cntr = MongoDbController.getInstance()

        mongo_cntr.create_resource(self.RESOURCE_ID, self.PRIMARY_KEY)
        mongo_cntr.update_schema(self.RESOURCE_ID, self.FIELDS)

        for f in self.FIELDS:
            if '_id' in f:
                f.pop('_id')

        self.assertTrue(mongo_cntr.resource_exists(self.RESOURCE_ID))

        self.DATA_RECORD[1]['field1'] = '1234'
        mongo_cntr.upsert(self.RESOURCE_ID, copy.deepcopy(self.DATA_RECORD), False)

        fields = mongo_cntr.resource_fields(self.RESOURCE_ID)

        self.assertEqual(fields['schema'], self.FIELDS)

        mongo_cntr.update_schema(self.RESOURCE_ID,
                                 [{'id': 'field1', 'type': 'number', 'info': {'type_override': 'float'}}])

        result = mongo_cntr.query_current_state(self.RESOURCE_ID, {}, None, None, 0, 0, False, True)

        self.assertEqual(type(result['records'][0]['field1']), unicode)
        self.assertEqual(type(result['records'][1]['field1']), float)

    def test_upsert(self):
        mongo_cntr = MongoDbController.getInstance()

        mongo_cntr.create_resource(self.RESOURCE_ID, self.PRIMARY_KEY)
        mongo_cntr.update_schema(self.RESOURCE_ID, self.FIELDS)

        self.assertTrue(mongo_cntr.resource_exists(self.RESOURCE_ID))

        mongo_cntr.upsert(self.RESOURCE_ID, copy.deepcopy(self.DATA_RECORD), False)

        result = mongo_cntr.query_current_state(self.RESOURCE_ID, {}, {'_id': 0},
                                                None, None, None, None, False)

        mongo_cntr.upsert(self.RESOURCE_ID, copy.deepcopy(self.DATA_RECORD_UPDATE), False)

        updated_result = mongo_cntr.query_current_state(self.RESOURCE_ID, {},
                                                        {'_id': 0},
                                                        None, None, None, None, False)

        self.assertEqual(len(result['records']), 3)
        self.assertEqual(len(updated_result['records']), 4)

        self.assertEqual(result['records'], self.DATA_RECORD)

        self.assertEqual(updated_result['records'], [{u'id': 1, u'field1': u'abc', u'field2': 123, 'distinct_field': 1},
                                                     {u'id': 2, u'field1': u'def', u'field2': 456, 'distinct_field': 1},
                                                     {u'id': 3, u'field1': u'new_value', u'field2': 321,
                                                      'distinct_field': 1},
                                                     {u'id': 4, u'field1': u'jkl', u'field2': 432,
                                                      'distinct_field': 2}])

        self.assertEqual(mongo_cntr.datastore.get_collection(self.RESOURCE_ID).count(), 5)

    def test_upsert_records_with_no_id(self):
        mongo_cntr = MongoDbController.getInstance()

        mongo_cntr.create_resource(self.RESOURCE_ID, self.PRIMARY_KEY)
        mongo_cntr.update_schema(self.RESOURCE_ID, self.FIELDS)

        self.assertTrue(mongo_cntr.resource_exists(self.RESOURCE_ID))

        mongo_cntr.upsert(self.RESOURCE_ID, copy.deepcopy(self.DATA_RECORD), False)
        self.assertRaises(MongoDbControllerException, mongo_cntr.upsert, self.RESOURCE_ID,
                          self.DATA_RECORD_INVALID_UPDATE, False)

    def test_retrieve_stored_query(self):
        mongo_cntr = MongoDbController.getInstance()

        mongo_cntr.create_resource(self.RESOURCE_ID, self.PRIMARY_KEY)
        mongo_cntr.update_schema(self.RESOURCE_ID, copy.deepcopy(self.FIELDS))

        self.assertTrue(mongo_cntr.resource_exists(self.RESOURCE_ID))

        mongo_cntr.upsert(self.RESOURCE_ID, copy.deepcopy(self.DATA_RECORD), False)

        result = mongo_cntr.query_current_state(self.RESOURCE_ID, {}, {u'_id': 0},
                                                None, None, None, False, True)

        mongo_cntr.upsert(self.RESOURCE_ID, copy.deepcopy(self.DATA_RECORD_UPDATE), False)

        mongo_cntr.delete_resource(self.RESOURCE_ID, {u'id': 1})

        new_result = mongo_cntr.query_current_state(self.RESOURCE_ID, {},
                                                    {u'_id': 0},
                                                    None, None, None, False, True)

        history_result = mongo_cntr.retrieve_stored_query(result[u'pid'], None, None)
        history_result_csv = mongo_cntr.retrieve_stored_query(result[u'pid'], None, None, 'csv')

        self.assertEqual(result[u'records'], self.DATA_RECORD)

        self.assertEqual(new_result[u'records'], [{u'id': 2, u'field1': u'def', u'field2': 456, 'distinct_field': 1},
                                                  {u'id': 3, u'field1': u'new_value', u'field2': 321,
                                                   'distinct_field': 1},
                                                  {u'id': 4, u'field1': u'jkl', u'field2': 432, 'distinct_field': 2}])

        self.assertEqual(history_result[u'records'], self.DATA_RECORD)

        self.assertEqual(history_result_csv[u'records'], self.DATA_RECORD_CSV)

    def test_retrieve_stored_query_with_projection(self):
        mongo_cntr = MongoDbController.getInstance()

        mongo_cntr.create_resource(self.RESOURCE_ID, self.PRIMARY_KEY)
        mongo_cntr.update_schema(self.RESOURCE_ID, self.FIELDS)

        self.assertTrue(mongo_cntr.resource_exists(self.RESOURCE_ID))

        mongo_cntr.upsert(self.RESOURCE_ID, copy.deepcopy(self.DATA_RECORD), False)

        result = mongo_cntr.query_current_state(self.RESOURCE_ID, {}, {u'_id': 0, 'field1': 1, 'id': 1},
                                                None, None, None, False, True)

        mongo_cntr.upsert(self.RESOURCE_ID, copy.deepcopy(self.DATA_RECORD_UPDATE), False)

        mongo_cntr.delete_resource(self.RESOURCE_ID, {u'id': 1})

        new_result = mongo_cntr.query_current_state(self.RESOURCE_ID, {},
                                                    {u'_id': 0, 'field1': 1, 'id': 1},
                                                    None, None, None, False, True)

        history_result = mongo_cntr.retrieve_stored_query(result[u'pid'], None, None)
        history_result_csv = mongo_cntr.retrieve_stored_query(result[u'pid'], None, None, 'csv')

        self.assertEqual(result[u'records'],
                         [{'field1': record['field1'], 'id': record['id']} for record in self.DATA_RECORD])

        self.assertEqual(new_result[u'records'], [{u'id': 2, u'field1': u'def'},
                                                  {u'id': 3, u'field1': u'new_value'},
                                                  {u'id': 4, u'field1': u'jkl'}])

        log.debug(result['pid'])

        self.assertEqual(history_result[u'records'],
                         [{'field1': record['field1'], 'id': record['id']} for record in self.DATA_RECORD])

        self.assertEqual(history_result_csv[u'records'], "abc;1\r\ndef;2\r\nghi;3\r\n")

    def test_retrieve_nonexisting_pid(self):
        mongo_cntr = MongoDbController.getInstance()
        self.assertRaises(QueryNotFoundException, mongo_cntr.retrieve_stored_query, 12344321, None, None)

    def test_query_current_state_with_sorting(self):
        mongo_cntr = MongoDbController.getInstance()

        mongo_cntr.create_resource(self.RESOURCE_ID, self.PRIMARY_KEY)
        mongo_cntr.update_schema(self.RESOURCE_ID, self.FIELDS)

        self.assertTrue(mongo_cntr.resource_exists(self.RESOURCE_ID))

        mongo_cntr.upsert(self.RESOURCE_ID,
                          copy.deepcopy(self.DATA_RECORD) + [
                              {'field2': 123, 'field1': 'abc', 'distinct_field': 1, 'id': 0}], False)

        result = mongo_cntr.query_current_state(self.RESOURCE_ID, {},
                                                {},
                                                [{'field1': 1}], None, None,
                                                False, True)

        result_csv = mongo_cntr.query_current_state(self.RESOURCE_ID, {},
                                                    {'_id': 0, 'id': 1, 'field1': 1},
                                                    [{'field1': 1}], None, None,
                                                    False, True, 'csv')

        self.assertTrue('pid' in result.keys())
        self.assertIsNotNone(result['pid'])

        self.assertEqual(result['records'],
                         [{'field1': 'abc', 'field2': 123, 'distinct_field': 1, 'id': 0}] + self.DATA_RECORD)

        self.assertEqual(result_csv['records'], 'abc;0\r\nabc;1\r\ndef;2\r\nghi;3\r\n')

    def test_query_by_id(self):
        mongo_cntr = MongoDbController.getInstance()

        mongo_cntr.create_resource(self.RESOURCE_ID, self.PRIMARY_KEY)
        mongo_cntr.update_schema(self.RESOURCE_ID, self.FIELDS)

        mongo_cntr.upsert(self.RESOURCE_ID,
                          copy.deepcopy(self.DATA_RECORD) + [
                              {'field2': 123, 'field1': 'abc', 'distinct_field': 1, 'id': 0}], False)

        result = mongo_cntr.query_current_state(self.RESOURCE_ID, {'id': 1},
                                                {},
                                                None, None, None,
                                                False, True)

        self.assertEqual(len(result['records']), 1)

        self.assertEqual(result['records'][0], self.DATA_RECORD[0])

    def test_distinct_query(self):
        mongo_cntr = MongoDbController.getInstance()
        mongo_cntr.create_resource(self.RESOURCE_ID, self.PRIMARY_KEY)
        mongo_cntr.update_schema(self.RESOURCE_ID, self.FIELDS)

        self.assertTrue(mongo_cntr.resource_exists(self.RESOURCE_ID))

        mongo_cntr.upsert(self.RESOURCE_ID, self.DATA_RECORD, False)
        mongo_cntr.upsert(self.RESOURCE_ID, self.DATA_RECORD_UPDATE, False)

        result = mongo_cntr.query_current_state(self.RESOURCE_ID, {}, {'_id': 0, 'distinct_field': 1},
                                                None, 0, 0, True, True)

        self.assertEqual(result['records'], [{'distinct_field': 1}, {'distinct_field': 2}])

    def test_pagination(self):
        mongo_cntr = MongoDbController.getInstance()
        mongo_cntr.create_resource(self.RESOURCE_ID, self.PRIMARY_KEY)
        mongo_cntr.update_schema(self.RESOURCE_ID, self.FIELDS)

        self.assertTrue(mongo_cntr.resource_exists(self.RESOURCE_ID))

        mongo_cntr.upsert(self.RESOURCE_ID, copy.deepcopy(self.DATA_RECORD), False)
        result = mongo_cntr.query_current_state(self.RESOURCE_ID, {}, {'_id': 0}, None, 1, 1, False, False)

        self.assertEqual(len(result['records']), 1)

        self.assertEqual(result['records'][0], self.DATA_RECORD[1])
