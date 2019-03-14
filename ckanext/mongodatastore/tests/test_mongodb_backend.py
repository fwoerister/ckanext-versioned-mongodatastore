import unittest

from mock import patch, MagicMock

from ckanext.mongodatastore.backend.mongodb import raise_exeption, transform_query_to_statement, \
    transform_filter, log_parameter_not_used_warning, MongoDataStoreBackend
from ckanext.mongodatastore.mongodb_controller import MongoDbController

SCHEMA = {'age': 'number', 'id': 'number', 'name': 'string'}


class MongoDbBackendTest(unittest.TestCase):

    def setUp(self):
        self.backend = MongoDataStoreBackend()

    def test_raise_exception(self):
        self.assertRaises(Exception, raise_exeption, Exception())

    def test_create_query_filter(self):
        query = transform_query_to_statement('1', SCHEMA)

        self.assertEqual(query, {'$or': [{'age': 1.0}, {'id': 1.0}, {'name': '1'}]})

    def test_transform_filter(self):
        filter = transform_filter({'id': [1, 2, 3], 'name': 'michael', 'age': '14'}, SCHEMA)
        self.assertEqual({'age': 14.0, 'id': {'$in': [1.0, 2.0, 3.0]}, 'name': 'michael'}, filter)

    @patch('ckanext.mongodatastore.backend.mongodb.log')
    def test_log_parameter_not_used_warning(self, mocked_log):
        log_parameter_not_used_warning([('param1', None), ('param2', 'value')])
        mocked_log.warn.assert_called_once()

    def test_init(self):
        self.assertIsNotNone(self.backend.mongo_cntr)

    def test_configure(self):
        backend = MongoDataStoreBackend()
        with patch.object(MongoDbController, 'reloadConfig', return_value=None) as mock_method:
            backend.configure({})
            mock_method.assert_called_with({})

    def test_create(self):
        d = {
            'resource_id': 'resource_123',
            'records': {'id': 1, 'name': 'florian', 'age': 27},
            'fields': [{'id': 'id', 'type': 'number'}, {'id': 'name', 'type': 'string'},
                       {'id': 'age', 'type': 'number'}],
            'primary_key': 'id'
        }

        cntr_mock = MagicMock()
        self.backend.mongo_cntr = cntr_mock

        self.backend.create({}, d)

        cntr_mock.create_resource.assert_called_with('resource_123', 'id')
        cntr_mock.upsert.assert_called_with('resource_123', {'id': 1, 'name': 'florian', 'age': 27})
        cntr_mock.update_datatypes.assert_not_called()

    def test_create_with_datatype_info(self):
        data_dict = {
            'resource_id': 'resource_123',
            'records': [{'id': 1, 'name': 'florian', 'age': 27}],
            'fields': [{'id': 'id', 'type': 'number'}, {'id': 'name', 'type': 'string'},
                       {'id': 'age', 'type': 'string', 'info': {'type_override': 'number'}}],
            'primary_key': 'id'
        }

        cntr_mock = MagicMock()
        self.backend.mongo_cntr = cntr_mock

        self.backend.create({}, data_dict)

        cntr_mock.create_resource.assert_called_with('resource_123', 'id')
        cntr_mock.upsert.assert_called_with('resource_123', [{'id': 1, 'name': 'florian', 'age': 27}])
        cntr_mock.update_datatypes.assert_called_with('resource_123',
                                                      [{'id': 'id', 'type': 'number'}, {'id': 'name', 'type': 'string'},
                                                       {'id': 'age', 'type': 'string',
                                                        'info': {'type_override': 'number'}}])

    def test_upsert(self):
        data_dict = {
            'resource_id': 'resource_123',
            'force': True,
            'records': [{'id': 1, 'name': 'florian', 'age': 27}],
            'method': 'upsert'
        }

        cntr_mock = MagicMock()
        self.backend.mongo_cntr = cntr_mock

        self.backend.upsert({}, data_dict)

        cntr_mock.upsert.assert_called_with('resource_123', [{'id': 1, 'name': 'florian', 'age': 27}], False)

    def test_upsert_with_not_supported_operation_insert(self):
        data_dict = {
            'resource_id': 'resource_123',
            'force': True,
            'records': [{'id': 1, 'name': 'florian', 'age': 27}],
            'method': 'insert'
        }

        cntr_mock = MagicMock()
        self.backend.mongo_cntr = cntr_mock

        self.assertRaises(NotImplementedError, self.backend.upsert, {}, data_dict)

    def test_upsert_with_not_supported_operation_update(self):
        data_dict = {
            'resource_id': 'resource_123',
            'force': True,
            'records': [{'id': 1, 'name': 'florian', 'age': 27}],
            'method': 'update'
        }

        cntr_mock = MagicMock()
        self.backend.mongo_cntr = cntr_mock

        self.assertRaises(NotImplementedError, self.backend.upsert, {}, data_dict)

    def test_delete(self):
        data_dict = {
            'resource_id': 'resource_123'
        }

        cntr_mock = MagicMock()
        self.backend.mongo_cntr = cntr_mock

        self.backend.delete({}, data_dict)

        cntr_mock.delete_resource.assert_called_with('resource_123', {}, force=False)

    def test_delete_with_filter(self):
        data_dict = {
            'resource_id': 'resource_123',
            'filters': {'id': 2}
        }

        cntr_mock = MagicMock()
        self.backend.mongo_cntr = cntr_mock

        self.backend.delete({}, data_dict)

        cntr_mock.delete_resource.assert_called_with('resource_123', {'id': 2}, force=False)

    def test_force_delete(self):
        data_dict = {
            'resource_id': 'resource_123',
            'force': True
        }

        cntr_mock = MagicMock()
        self.backend.mongo_cntr = cntr_mock

        self.backend.delete({}, data_dict)

        cntr_mock.delete_resource.assert_called_with('resource_123', {}, force=True)

    def test_search(self):
        data_dict = {
            'resource_id': 'resource_123',
            'filters': {'name': 'florian', 'age': 20}
        }

        cntr_mock = MagicMock()
        cntr_mock.query_current_state.return_value = {'fields': None, 'pid': 123}

        self.backend.mongo_cntr = cntr_mock

        self.backend.search({}, data_dict)

        cntr_mock.query_current_state.assert_called_with('resource_123', {'age': 20, 'name': 'florian'}, {}, None, 0,
                                                         100, False, True, 'objects')

    def test_search_sql(self):
        self.assertRaises(NotImplementedError, self.backend.search_sql, {}, {})

    def test_resource_exists(self):
        cntr_mock = MagicMock()
        self.backend.mongo_cntr = cntr_mock

        self.backend.resource_exists('resource_id')

        cntr_mock.resource_exists.assert_called_with('resource_id')

    def test_resource_info(self):
        cntr_mock = MagicMock()
        self.backend.mongo_cntr = cntr_mock

        self.backend.resource_fields('resource_id')

        cntr_mock.resource_fields.assert_called_with('resource_id')

    def test_resource_id_from_alias(self):
        cntr_mock = MagicMock()
        self.backend.mongo_cntr = cntr_mock
        cntr_mock.resource_exists.return_value = True

        boolean_val, alias = self.backend.resource_id_from_alias('alias')

        self.assertTrue(boolean_val)
        self.assertEqual(alias, 'alias')

        cntr_mock.resource_exists.return_value = False

        boolean_val, alias = self.backend.resource_id_from_alias('alias')

        self.assertFalse(boolean_val)
        self.assertEqual(alias, 'alias')

    def test_get_all_ids(self):
        cntr_mock = MagicMock()
        self.backend.mongo_cntr = cntr_mock

        self.backend.get_all_ids()

        cntr_mock.get_all_ids.assert_called_once()

    def test_create_function(self):
        self.assertRaises(NotImplementedError, self.backend.create_function)

    def test_drop_function(self):
        self.assertRaises(NotImplementedError, self.backend.drop_function)
