"""Tests for plugin.py."""
import unittest

from ckan.plugins import toolkit
from mock import patch, MagicMock

import ckanext.mongodatastore.plugin as plugin


class PluginTest(unittest.TestCase):
    def setUp(self):
        self.plugin = plugin.MongodatastorePlugin()

    def test_update_config(self):
        with patch.object(toolkit, 'add_template_directory', return_value=None) as mock_method:
            self.plugin.update_config({})
            mock_method.assert_called()

        with patch.object(toolkit, 'add_resource', return_value=None) as mock_method:
            self.plugin.update_config({})
            mock_method.assert_called()

    def test_register_datasource(self):
        return_val = self.plugin.register_datasource()
        self.assertIsNotNone(return_val['postgres'])

    def test_register_backends(self):
        return_val = self.plugin.register_backends()
        print(return_val)
        self.assertIsNotNone(return_val['mongodb'])

    def test_before_map(self):
        mock = MagicMock()
        self.plugin.before_map(mock)

        mock.connect.assert_any_call('resource_import_table', '/mongodatastore/import',
                                     controller='ckanext.mongodatastore.controller:MongoDatastoreController',
                                     action='import_table')

        mock.connect.assert_any_call('resource_importer', '/dataset/{id}/import/{resource_id}',
                                     controller='ckanext.mongodatastore.controller:MongoDatastoreController',
                                     action='show_import', ckan_icon='download')

        mock.connect.assert_any_call('querystore.dump', '/querystore/dump_history_result_set',
                                     controller='ckanext.mongodatastore.controller:QueryStoreController',
                                     action='dump_history_result_set')

        mock.connect.assert_any_call('querystore.view', '/querystore/view_query',
                                     controller='ckanext.mongodatastore.controller:QueryStoreController',
                                     action='view_history_query')

    def test_actions(self):
        actions = self.plugin.get_actions()
        self.assertIsNotNone(actions['querystore_resolve'])
        self.assertIsNotNone(actions['datastore_restore'])
