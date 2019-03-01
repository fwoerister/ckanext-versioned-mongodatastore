import unittest

from mock import patch, MagicMock

from ckanext.mongodatastore.logic.action import querystore_resolve


class ActionTest(unittest.TestCase):

    @patch('ckanext.mongodatastore.mongodb_controller.MongoDbController.getInstance')
    def test_querystore_resolve(self, get_instance_mock):
        mongodb_cntr_mock = MagicMock()
        get_instance_mock.return_value = mongodb_cntr_mock

        data_dict = {'pid': 123, 'offset': 0, 'limit': 100, 'records_format': 'list'}

        querystore_resolve({}, data_dict)

        mongodb_cntr_mock.execute_stored_query.assert_called_with(123, offset=0, limit=100, records_format='list')

    @patch('ckanext.mongodatastore.mongodb_controller.MongoDbController.getInstance')
    def test_querystore_resolve_default_values(self, get_instance_mock):
        mongodb_cntr_mock = MagicMock()
        get_instance_mock.return_value = mongodb_cntr_mock

        data_dict = {'pid': 123}

        querystore_resolve({}, data_dict)

        mongodb_cntr_mock.execute_stored_query.assert_called_with(123, offset=None, limit=None,
                                                                  records_format='objects')
