import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit

from ckanext.datastore.interfaces import IDatastoreBackend

from ckanext.mongodatastore.datasource import DataSourceAdapter
from ckanext.mongodatastore.interfaces import IDataSourceAdapter

from ckanext.mongodatastore.datasource.mariadb import MariaDbDatasource
from ckanext.mongodatastore.datasource.postgresql import PostgreSqlDatasource

from ckanext.mongodatastore.logic.action import querystore_resolve, datastore_restore
from ckanext.mongodatastore.backend.mongodb import MongoDataStoreBackend


class MongodatastorePlugin(plugins.SingletonPlugin):
    plugins.implements(IDatastoreBackend)
    plugins.implements(plugins.IConfigurer)
    plugins.implements(plugins.IRoutes, inherit=True)
    plugins.implements(plugins.IActions)
    plugins.implements(IDataSourceAdapter, inherit=True)

    def update_config(self, config_):
        toolkit.add_template_directory(config_, 'templates')
        toolkit.add_resource('public', 'ckanext-mongodatastore')
        DataSourceAdapter.register_datasource()

    # IDataSourceAdapter
    def register_datasource(self):
        return {
            'postgres': PostgreSqlDatasource,
        }

    # IDatastoreBackend
    def register_backends(self):
        return {
            u'mongodb': MongoDataStoreBackend
        }

    # IRoutes
    def before_map(self, m):
        m.connect('querystore.view', '/querystore/view_query',
                  controller='ckanext.mongodatastore.controller:QueryStoreController',
                  action='view_history_query')

        m.connect('querystore.dump', '/querystore/dump_history_result_set',
                  controller='ckanext.mongodatastore.controller:QueryStoreController',
                  action='dump_history_result_set')

        m.connect('resource_importer', '/dataset/{id}/import/{resource_id}',
                  controller='ckanext.mongodatastore.controller:MongoDatastoreController',
                  action='show_import', ckan_icon='download')

        m.connect('resource_import_table', '/mongodatastore/import',
                  controller='ckanext.mongodatastore.controller:MongoDatastoreController',
                  action='import_table')

        return m

    # IActions
    def get_actions(self):
        actions = {
            'querystore_resolve': querystore_resolve,
            'datastore_restore': datastore_restore
        }

        return actions
