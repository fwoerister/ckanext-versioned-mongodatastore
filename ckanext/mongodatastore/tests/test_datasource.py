import unittest

from mock import patch, MagicMock

from ckanext.mongodatastore.datasource import DataSourceAdapter


class DataSourceAdapterTest(unittest.TestCase):

    @patch('ckanext.mongodatastore.datasource.plugins')
    def test_register_datasource(self, plugins_mock):

        plugin_mock = MagicMock()
        plugins_mock.PluginImplementations.return_value = [plugin_mock]

        DataSourceAdapter.register_datasource()

        plugin_mock.register_datasource.assert_called()
