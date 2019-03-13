import ckan.plugins.interfaces as interfaces


class IDataSourceAdapter(interfaces.Interface):

    def register_datasource(self):
        return {}
