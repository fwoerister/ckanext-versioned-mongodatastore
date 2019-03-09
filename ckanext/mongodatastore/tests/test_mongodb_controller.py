import unittest

from ckanext.mongodatastore.helper import CKAN_DATASTORE
from ckanext.mongodatastore.mongodb_controller import convert_to_csv, MongoDbController, \
    MongoDbControllerException, QueryNotFoundException

PRIMARY_KEY = 'id'
RESOURCE_ID = 'test_resource_id'

DATA_RECORD = [
    {u'id': 1, u'field1': u'abc', u'field2': 123},
    {u'id': 2, u'field1': u'def', u'field2': 456},
    {u'id': 3, u'field1': u'ghi', u'field2': 456}
]

DATA_RECORD_KEYS = ['field1', 'field2', 'id']
DATA_RECORD_CSV = "abc;123;1\r\ndef;456;2\r\nghi;456;3\r\n"

DATA_RECORD_UPDATE = [
    {u'id': 3, u'field1': u'new_value', u'field2': 321},
    {u'id': 4, u'field1': u'jkl', u'field2': 432}
]

DATA_RECORD_INVALID_UPDATE = [
    {u'field1': u'abc', u'field2': u'abc'}
]


class MongoDbControllerTest(unittest.TestCase):

    def setUp(self):
        instance = MongoDbController.getInstance()
        instance.client.drop_database(CKAN_DATASTORE)
        instance.datastore = instance.client.get_database(CKAN_DATASTORE)

    # helper function tests

    def test_convert_to_csv(self):
        result = str(convert_to_csv(DATA_RECORD, DATA_RECORD_KEYS))
        self.assertEqual(result, DATA_RECORD_CSV)

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

        mongo_cntr.create_resource(RESOURCE_ID, PRIMARY_KEY)
        all_ids = mongo_cntr.get_all_ids()

        self.assertIn(RESOURCE_ID, all_ids)
        self.assertIn('{0}_meta'.format(RESOURCE_ID), mongo_cntr.datastore.list_collection_names())

    def test_empty_resource(self):
        mongo_cntr = MongoDbController.getInstance()

        mongo_cntr.create_resource(RESOURCE_ID, PRIMARY_KEY)

        self.assertTrue(mongo_cntr.resource_exists(RESOURCE_ID))

        mongo_cntr.delete_resource(RESOURCE_ID, None)

        self.assertTrue(mongo_cntr.resource_exists(RESOURCE_ID))

        mongo_cntr.delete_resource(RESOURCE_ID, None, force=True)

        self.assertFalse(mongo_cntr.resource_exists(RESOURCE_ID))

    def test_resource_with_records(self):
        mongo_cntr = MongoDbController.getInstance()

        mongo_cntr.create_resource(RESOURCE_ID, PRIMARY_KEY)

        self.assertTrue(mongo_cntr.resource_exists(RESOURCE_ID))

        mongo_cntr.upsert(RESOURCE_ID, DATA_RECORD, False)

        col = mongo_cntr.datastore.get_collection(RESOURCE_ID)
        self.assertNotEqual(col.count_documents({}), 0)

        mongo_cntr.delete_resource(RESOURCE_ID, None)

        self.assertEqual(col.count_documents({}), 6)

        self.assertTrue(mongo_cntr.resource_exists(RESOURCE_ID))

        mongo_cntr.delete_resource(RESOURCE_ID, None, force=True)

        self.assertFalse(mongo_cntr.resource_exists(RESOURCe_ID))

    def test_update_datatypes(self):
        mongo_cntr = MongoDbController.getInstance()

        mongo_cntr.create_resource(RESOURCE_ID, PRIMARY_KEY)

        self.assertTrue(mongo_cntr.resource_exists(RESOURCE_ID))

        mongo_cntr.upsert(RESOURCE_ID, DATA_RECORD, False)

        fields = mongo_cntr.resource_fields(RESOURCE_ID)

        self.assertEqual(fields['schema'].keys(), DATA_RECORD_KEYS)

        mongo_cntr.update_datatypes(RESOURCE_ID, [{'id': 'field1', 'info': {'type_override': 'number'}}])

        fields = mongo_cntr.resource_fields(RESOURCE_ID)

        self.assertEqual(fields['schema'].keys(), ['field1', 'field2', 'id'])

    def test_update_datatypes_with_value_error(self):
        mongo_cntr = MongoDbController.getInstance()

        mongo_cntr.create_resource(RESOURCe_ID, PRIMARY_KEY)

        self.assertTrue(mongo_cntr.resource_exists(RESOURCE_ID))

        mongo_cntr.upsert(RESOURCE_ID, DATA_RECORD, False)

        fields = mongo_cntr.resource_fields(RESOURCE_ID)

        self.assertEqual(fields['schema'].keys(), ['field1', 'field2', 'id'])

        mongo_cntr.update_datatypes(RESOURCE_ID, [{'id': 'field1', 'info': {'type_override': 'number'}}])

        result = mongo_cntr.query_current_state(RESOURCE_ID, {}, None, None, 0, 0, False, True)

        self.assertEqual(type(result['records'][0]['field1']), unicode)
        self.assertEqual(type(result['records'][1]['field1']), float)

    def test_upsert(self):
        mongo_cntr = MongoDbController.getInstance()

        mongo_cntr.create_resource(RESOURCE_ID, PRIMARY_KEY)

        self.assertTrue(mongo_cntr.resource_exists(RESOURCE_ID))

        mongo_cntr.upsert(RESOURCE_ID, DATA_RECORD, False)

        result = mongo_cntr.query_current_state(RESOURCE_ID, {}, {'_id': 0, 'id': 1, 'field1': 1, 'field2': 1},
                                                None, None, None, None, False)

        mongo_cntr.upsert(RESOURCE_ID, DATA_RECORD_UPDATE, False)

        updated_result = mongo_cntr.query_current_state(RESOURCE_ID, {},
                                                        {'_id': 0, 'id': 1, 'field1': 1, 'field2': 1},
                                                        None, None, None, None, False)

        self.assertEqual(len(result['records']), 2)
        self.assertEqual(len(updated_result['records']), 3)

        self.assertEqual(result['records'][0], {'id': 1, 'field1': 'abc', 'field2': 123})
        self.assertEqual(result['records'][1], {'id': 2, 'field1': 'def', 'field2': 456})

        self.assertEqual(updated_result['records'][0], {'id': 1, 'field1': 'abc', 'field2': 123})
        self.assertEqual(updated_result['records'][1], {'id': 2, 'field1': 'new_value', 'field2': 1})
        self.assertEqual(updated_result['records'][2], {'id': 3, 'field1': 'ghi', 'field2': 1})

    def test_upsert_records_with_no_id(self):
        mongo_cntr = MongoDbController.getInstance()

        mongo_cntr.create_resource(RESOURCE_ID, PRIMARY_KEY)

        self.assertTrue(mongo_cntr.resource_exists(RESOURCE_ID))

        mongo_cntr.upsert(RESOURCE_ID, DATA_RECORD, False)
        self.assertRaises(MongoDbControllerException, mongo_cntr.upsert, RESOURCE_ID, DATA_RECORD_INVALID_UPDATE, False)

    def test_retrieve_stored_query(self):
        mongo_cntr = MongoDbController.getInstance()

        mongo_cntr.create_resource(RESOURCE_ID, PRIMARY_KEY)

        self.assertTrue(mongo_cntr.resource_exists(RESOURCE_ID))

        mongo_cntr.upsert(RESOURCE_ID, DATA_RECORD, False)

        result = mongo_cntr.query_current_state(RESOURCE_ID, {}, {u'_id': 0, u'id': 1, u'field1': 1, u'field2': 1},
                                                None, None, None, False, True)

        mongo_cntr.upsert(RESOURCE_ID, DATA_RECORD_UPDATE, False)

        mongo_cntr.delete_resource(RESOURCE_ID, {u'id': 1})

        new_result = mongo_cntr.query_current_state(RESOURCE_ID, {},
                                                    {u'_id': 0, u'id': 1, u'field1': 1, u'field2': 1},
                                                    None, None, None, False, True)

        history_result = mongo_cntr.retrieve_stored_query(result[u'pid'], None, None)
        history_result_csv = mongo_cntr.retrieve_stored_query(result[u'pid'], None, None, 'csv')

        self.assertEqual(result[u'records'], DATA_RECORD)

        self.assertEqual(new_result[u'records'], [{u'id': 2, u'field1': u'def', u'field2': 456},
                                                  {u'id': 3, u'field1': u'new_value', u'field2': 321},
                                                  {u'id': 4, u'field1': u'jkl', u'field2': 432}])

        self.assertEqual(history_result[u'records'], DATA_RECORD)

        self.assertEqual(history_result_csv[u'records'], DATA_RECORD_CSV)

    def test_retrieve_nonexisting_pid(self):
        mongo_cntr = MongoDbController.getInstance()
        self.assertRaises(QueryNotFoundException, mongo_cntr.retrieve_stored_query, 12344321, None, None)

    def test_query_current_state_with_sorting(self):
        mongo_cntr = MongoDbController.getInstance()

        mongo_cntr.create_resource(RESOURCE_ID, PRIMARY_KEY)

        self.assertTrue(mongo_cntr.resource_exists(RESOURCE_ID))

        mongo_cntr.upsert(RESOURCE_ID, DATA_RECORD + [{'field1': 'abc', 'field2': 123, 'id': 0}], False)

        result = mongo_cntr.query_current_state(RESOURCE_ID, {},
                                                {'_id': 0, 'id': 1, 'field1': 1, 'field2': 1},
                                                [{'field1': 1}], None, None,
                                                False, True)

        print(result['records'])

        self.assertTrue('pid' in result.keys())
        self.assertIsNotNone(result['pid'])

        self.assertEqual(result['records'], [{'field1': 'abc', 'field2': 123, 'id': 0}]+DATA_RECORD)