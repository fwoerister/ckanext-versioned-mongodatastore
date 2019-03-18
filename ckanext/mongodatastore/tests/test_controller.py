import unittest
import StringIO

from mock import patch, MagicMock

import ckan
from ckanext.mongodatastore.controller import history_dump_to, MongoDatastoreController, QueryStoreController

mock_xml = MagicMock()
mock_xml.side_effect = [{'limit': 1, 'fields': [{'id': 'name', 'type': 'string'}], 'records': [{'name': 'florian'}]},
                        {'limit': 1, 'fields': [{'id': 'name', 'type': 'string'}], 'records': []}]

mock_json = MagicMock()
mock_json.side_effect = [{'limit': 1, 'fields': [{'id': 'name', 'type': 'string'}], 'records': [{'name': 'florian'}]},
                         {'limit': 1, 'fields': [{'id': 'name', 'type': 'string'}], 'records': []}]

mock_csv = MagicMock()
mock_csv.side_effect = [
    {'limit': 1, 'fields': [{'id': 'id', 'type': 'number'}, {'id': 'name', 'type': 'string'}], 'records': '1,florian'},
    {'limit': 1, 'fields': [{'id': 'id', 'type': 'number'}, {'id': 'name', 'type': 'string'}], 'records': None}]


class ControllerTest(unittest.TestCase):

    @patch('ckanext.mongodatastore.controller.get_action', return_value=mock_xml)
    def test_history_dump_to_xml(self, mock):
        test_stream = StringIO.StringIO()

        history_dump_to(1, test_stream, 'xml', 0, 1, {})

        print(test_stream.getvalue())

        self.assertEqual(test_stream.getvalue(), '<data>\n<row><name>florian</name></row>\n</data>\n')

    @patch('ckanext.mongodatastore.controller.get_action', return_value=mock_json)
    def test_history_dump_to_json(self, mock):
        test_stream = StringIO.StringIO()

        history_dump_to(1, test_stream, 'json', 0, 1, {})

        print(test_stream.getvalue())

        self.assertEqual(test_stream.getvalue(),
                         u'{\n  "fields": [{"type":"string","id":"name"}],\n  '
                         u'"records": [\n    {"name":"florian"}\n]}\n')

    @patch('ckanext.mongodatastore.controller.get_action', return_value=mock_csv)
    def test_history_dump_to_csv(self, mock):
        test_stream = StringIO.StringIO()

        history_dump_to(1, test_stream, 'csv', 0, 1, {})

        print(test_stream.getvalue())

        self.assertEqual(test_stream.getvalue(), 'id,name\r\n1,florian')


class MongoDatastoreControllerTest(unittest.TestCase):
    def setUp(self):
        self.cntr = MongoDatastoreController()

        self.pkg = {u'license_title': u'Creative Commons Attribution', u'maintainer': u'',
                    u'relationships_as_object': [], u'private': False, u'maintainer_email': u'',
                    u'num_tags': 4, u'id': u'accae748-7980-424b-b4d3-707a4aab3aad',
                    u'metadata_created': u'2019-02-16T17:14:55.852749',
                    u'metadata_modified': u'2019-03-14T22:45:17.223402',
                    u'author': u'Florian W\xf6rister',
                    u'author_email': u'e1126205@student.tuwien.ac.at', u'state': u'active',
                    u'version': u'1.0',
                    u'creator_user_id': u'fb76386d-5acf-464c-a5ae-f9395db837bc',
                    u'type': u'dataset', u'resources': [
                {u'mimetype': None, u'cache_url': None, u'hash': u'', u'description': u'',
                 u'name': u'query_store', u'format': u'database',
                 u'url': u'postgres://query_store:query_store@localhost/query_store', u'datastore_active': True,
                 u'cache_last_updated': None, u'package_id': u'accae748-7980-424b-b4d3-707a4aab3aad',
                 u'created': u'2019-03-14T22:45:17.252441', u'state': u'active', u'mimetype_inner': None,
                 u'last_modified': None, u'position': 0,
                 u'revision_id': u'482b4bef-a234-43eb-961c-c57f1353226c', u'url_type': None,
                 u'id': u'1fefeb77-2462-44e9-a2e6-9f2d7841bbdb', u'resource_type': None, u'size': None}],
                    u'num_resources': 1, u'tags': [
                {u'vocabulary_id': None, u'state': u'active', u'display_name': u'Datasets',
                 u'id': u'03281c9d-1ace-433a-bac1-6e040fdc11f0', u'name': u'Datasets'},
                {u'vocabulary_id': None, u'state': u'active', u'display_name': u'ML',
                 u'id': u'a7033efd-9dd7-4187-868e-c351ebbb2e81', u'name': u'ML'},
                {u'vocabulary_id': None, u'state': u'active', u'display_name': u'Trainset',
                 u'id': u'11c72095-20e2-43ad-abff-54f7f7d726e7', u'name': u'Trainset'},
                {u'vocabulary_id': None, u'state': u'active', u'display_name': u'UCI',
                 u'id': u'722f9849-45e8-4f7d-b120-2e15ec4cfb93', u'name': u'UCI'}], u'groups': [],
                    u'license_id': u'cc-by', u'relationships_as_subject': [],
                    u'organization': {u'description': u'', u'title': u'TU Wien',
                                      u'created': u'2019-02-07T15:41:16.124169',
                                      u'approval_status': u'approved', u'is_organization': True,
                                      u'state': u'active', u'image_url': u'',
                                      u'revision_id': u'19d5fd25-78a7-4885-92b2-590c00ee7fac',
                                      u'type': u'organization',
                                      u'id': u'24f09562-335d-4a1e-8717-de2829fca95e',
                                      u'name': u'tu-wien'}, u'name': u'uci-ml-datasets',
                    u'isopen': True, u'url': u'https://archive.ics.uci.edu/ml/index.php',
                    u'notes': u'We currently maintain 468 data sets as a service to the '
                              u'machine learning community. You may view all data sets '
                              u'through our searchable interface. For a general overview '
                              u'of the Repository, please visit our About page. For information '
                              u'about citing data sets in publications, please read our citation '
                              u'policy. If you wish to donate a data set, please consult our donation '
                              u'policy. For any other questions, feel free to contact the Repository '
                              u'librarians.',
                    u'owner_org': u'24f09562-335d-4a1e-8717-de2829fca95e', u'extras': [],
                    u'license_url': u'http://www.opendefinition.org/licenses/cc-by',
                    u'title': u'UCI ML Datasets',
                    u'revision_id': u'29799729-6842-4041-a96c-5c5fd1b19f9d'}

        self.res = {u'mimetype': None, u'cache_url': None, u'hash': u'', u'description': u'',
                    u'name': u'query_store', u'format': u'database',
                    u'url': u'postgres://query_store:query_store@localhost/query_store', u'datastore_active': True,
                    u'cache_last_updated': None, u'package_id': u'accae748-7980-424b-b4d3-707a4aab3aad',
                    u'created': u'2019-03-14T22:45:17.252441', u'state': u'active', u'mimetype_inner': None,
                    u'last_modified': None, u'position': 0, u'revision_id': u'482b4bef-a234-43eb-961c-c57f1353226c',
                    u'url_type': None, u'id': u'1fefeb77-2462-44e9-a2e6-9f2d7841bbdb', u'resource_type': None,
                    u'size': None}

    @patch('ckanext.mongodatastore.controller.render')
    @patch('ckanext.mongodatastore.controller.get_action')
    @patch('ckanext.mongodatastore.controller.DataSourceAdapter.get_datasource_adapter')
    @patch('ckanext.mongodatastore.controller.c')
    def test_show_import(self, c_mock, get_datasource_adapter_mock, get_action_mock, render_mock):
        public_tables = ['dataset1', 'dataset2']

        def action(name):
            if name == 'package_show':
                return lambda c, d: self.pkg
            elif name == 'resource_show':
                return lambda c, d: self.res

        get_action_mock.side_effect = action

        datasource_adapter_mock = MagicMock()
        datasource_adapter_mock.is_reachable.return_value = True
        datasource_adapter_mock.get_available_datasets.return_value = public_tables

        get_datasource_adapter_mock.return_value = datasource_adapter_mock
        self.cntr.show_import(1, 'resource_id')

        render_mock.assert_called_with('mongodatastore/import_rdb.html',
                                       extra_vars={
                                           'pkg_dict': self.pkg,
                                           'resource': self.res,
                                           'reachable': True,
                                           'public_tables': public_tables
                                       })

    @patch('ckanext.mongodatastore.controller.h')
    @patch('ckanext.mongodatastore.controller.get_action')
    @patch('ckanext.mongodatastore.controller.DataSourceAdapter.get_datasource_adapter')
    def test_import_table_with_existing_resource(self, get_datasource_adapter_mock, get_action_mock, h_mock):

        def get_param(name):
            if name == 'resource_id':
                return 'resource_id'
            elif name == 'table':
                return 'test_data'
            elif name == 'method':
                return 'upsert'
            else:
                return None

        h_mock.get_request_param.side_effect = get_param

        datastore_create_mock = MagicMock()

        def action(name):
            if name == 'datastore_search':
                return MagicMock()
            if name == 'resource_show':
                return lambda c, d: self.res
            if name == 'datastore_info':
                return lambda c, d: {'meta': {'record_id': 'id'}}
            if name == 'datastore_create':
                return datastore_create_mock

        get_action_mock.side_effect = action

        datasource_adapter_mock = MagicMock()
        get_datasource_adapter_mock.return_value = datasource_adapter_mock

        datasource_adapter_mock.get_primary_key_name.return_value = 'id'

        self.cntr.import_table()

        datastore_create_mock.assert_not_called()

        datasource_adapter_mock.migrate_records_to_datasource.assert_called()

        h_mock.flash_error.assert_not_called()
        h_mock.flash_success.assert_called()
        h_mock.redirect_to.assert_called_with('/')

    @patch('ckanext.mongodatastore.controller.h')
    @patch('ckanext.mongodatastore.controller.get_action')
    @patch('ckanext.mongodatastore.controller.DataSourceAdapter.get_datasource_adapter')
    def test_import_table_with_none_existing_resource(self, get_datasource_adapter_mock, get_action_mock, h_mock):

        def get_param(name):
            if name == 'resource_id':
                return 'resource_id'
            elif name == 'table':
                return 'test_data'
            elif name == 'method':
                return 'upsert'
            else:
                return None

        h_mock.get_request_param.side_effect = get_param

        datastore_create_mock = MagicMock()

        def action(name):
            if name == 'datastore_search':
                raise ckan.plugins.toolkit.ObjectNotFound()
            if name == 'resource_show':
                return lambda c, d: self.res
            if name == 'datastore_info':
                return lambda c, d: {'meta': {'record_id': 'id'}}
            if name == 'datastore_create':
                return datastore_create_mock

        get_action_mock.side_effect = action

        datasource_adapter_mock = MagicMock()
        get_datasource_adapter_mock.return_value = datasource_adapter_mock

        datasource_adapter_mock.get_primary_key_name.return_value = 'id'

        self.cntr.import_table()

        datastore_create_mock.assert_called()
        datasource_adapter_mock.migrate_records_to_datasource.assert_called()

        h_mock.flash_error.assert_not_called()
        h_mock.flash_success.assert_called()
        h_mock.redirect_to.assert_called_with('/')

    @patch('ckanext.mongodatastore.controller.h')
    @patch('ckanext.mongodatastore.controller.get_action')
    @patch('ckanext.mongodatastore.controller.DataSourceAdapter.get_datasource_adapter')
    def test_import_table_with_none_matching_ids(self, get_datasource_adapter_mock, get_action_mock, h_mock):

        def get_param(name):
            if name == 'resource_id':
                return 'resource_id'
            elif name == 'table':
                return 'test_data'
            elif name == 'method':
                return 'upsert'
            else:
                return None

        h_mock.get_request_param.side_effect = get_param

        datastore_create_mock = MagicMock()

        def action(name):
            if name == 'datastore_search':
                return MagicMock()
            if name == 'resource_show':
                return lambda c, d: self.res
            if name == 'datastore_info':
                return lambda c, d: {'meta': {'record_id': 'id'}}
            if name == 'datastore_create':
                return datastore_create_mock

        get_action_mock.side_effect = action

        datasource_adapter_mock = MagicMock()
        get_datasource_adapter_mock.return_value = datasource_adapter_mock

        datasource_adapter_mock.get_primary_key_name.return_value = 'other_id'

        self.cntr.import_table()

        datasource_adapter_mock.migrate_records_to_datasource.assert_not_called()

        h_mock.flash_error.assert_called()
        h_mock.flash_success.assert_not_called()
        h_mock.redirect_to.assert_called_with('/')


class QueryStoreControllerTest(unittest.TestCase):

    def setUp(self):
        self.cntr = QueryStoreController()

    @patch('ckanext.mongodatastore.controller.h')
    @patch('ckanext.mongodatastore.controller.render')
    @patch('ckanext.mongodatastore.controller.get_action')
    def test_view_history_query(self, get_action_mock, render_mock, h_mock):

        def get_param(name):
            if name == 'id':
                return 123
            return None

        def get_action(name):
            if name == 'querystore_resolve':
                return lambda c, d: {'records': [{'id': 1, 'name': 'florian'}], 'query': '[QUERY]',
                                     'fields': ['id', 'name']}

        h_mock.get_param_int.side_effect = get_param
        get_action_mock.side_effect = get_action

        self.cntr.view_history_query()

        render_mock.assert_called_with('mongodatastore/query_view.html', extra_vars={'query': '[QUERY]',
                                                                                     'result_set': [
                                                                                         {'id': 1, 'name': 'florian'}],
                                                                                     'count': 1,
                                                                                     'projection': ['id', 'name']})

    @patch('ckanext.mongodatastore.controller.h')
    @patch('ckanext.mongodatastore.controller.history_dump_to')
    @patch('ckanext.mongodatastore.controller.response')
    def test_dump_history_result_set(self, response_mock, history_dump_to_mock, h_mock):
        def get_param(name):
            if name == 'id':
                return '123'
            if name == 'format':
                return 'csv'
            if name == 'offset':
                return '0'
            if name == 'limit':
                return '100'
            if name == 'bom':
                return 'True'
            return None

        h_mock.get_request_param.side_effect = get_param
        self.cntr.dump_history_result_set()

        history_dump_to_mock.assert_called_with(123, response_mock, fmt='csv', offset=0, limit=100,
                                                options={u'bom': True})

    @patch('ckanext.mongodatastore.controller.h')
    @patch('ckanext.mongodatastore.controller.history_dump_to')
    @patch('ckanext.mongodatastore.controller.response')
    def test_dump_history_result_set_default_settings(self, response_mock, history_dump_to_mock, h_mock):
        def get_param(name):
            if name == 'id':
                return '123'
            if name == 'format':
                return 'csv'
            return None

        h_mock.get_request_param.side_effect = get_param
        self.cntr.dump_history_result_set()

        history_dump_to_mock.assert_called_with(123, response_mock, fmt='csv', offset=None, limit=None,
                                                options={u'bom': False})
