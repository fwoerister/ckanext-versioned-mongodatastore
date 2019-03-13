import unittest
from mock import patch, MagicMock

from ckanext.mongodatastore.backend.mongodb import create_projection, raise_exeption, transform_query_to_statement, \
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

    def test_delete(self):
        pass
