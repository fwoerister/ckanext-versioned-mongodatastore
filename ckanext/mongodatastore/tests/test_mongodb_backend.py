import unittest
from mock import patch

from ckanext.mongodatastore.backend.mongodb import create_projection, raise_exeption, transform_query_to_statement, \
    transform_filter, log_parameter_not_used_warning, MongoDataStoreBackend
from ckanext.mongodatastore.mongodb_controller import MongoDbController

SCHEMA = {'age': 'number', 'id': 'number', 'name': 'string'}


class MongoDbBackendTest(unittest.TestCase):

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
        backend = MongoDataStoreBackend()
        self.assertIsNotNone(backend.mongo_cntr)

    def test_configure(self):
        backend = MongoDataStoreBackend()
        with patch.object(MongoDbController, 'reloadConfig', return_value=None) as mock_method:
            backend.configure({})
            mock_method.assert_called_with({})