import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit
from ckanext.datastore.interfaces import IDatastoreBackend

from ckanext.mongodatastore.datastore_backend import MongoDataStoreBackend
from ckanext.mongodatastore.logic.action import querystore_resolve


class MongodatastorePlugin(plugins.SingletonPlugin):
    plugins.implements(IDatastoreBackend)
    plugins.implements(plugins.IConfigurer)
    plugins.implements(plugins.IRoutes, inherit=True)
    plugins.implements(plugins.IActions)

    def update_config(self, config):
        toolkit.add_public_directory(config, 'theme/public')
        toolkit.add_template_directory(config, 'theme/templates')
        toolkit.add_resource('theme/public', 'ckanext-mongodatastore')

    # IDatastoreBackend
    def register_backends(self):
        return {
            u'mongodb': MongoDataStoreBackend,
            u'mongodb+srv': MongoDataStoreBackend,
        }

    # IRoutes
    def before_map(self, m):
        m.connect('querystore.view', '/querystore/view_query',
                  controller='ckanext.mongodatastore.controller.ui_controller:QueryStoreUIController',
                  action='view_history_query')

        m.connect('querystore.dump', '/querystore/dump_history_result_set',
                  controller='ckanext.mongodatastore.controller.ui_controller:QueryStoreUIController',
                  action='dump_history_result_set')

        return m

    # IActions
    def get_actions(self):
        actions = {
            'querystore_resolve': querystore_resolve
        }

        return actions
