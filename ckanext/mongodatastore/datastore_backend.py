import logging
from collections import OrderedDict
from json import JSONDecoder

from ckan.logic import get_action

from ckan.lib.base import abort
from ckanext.datastore.backend import DatastoreBackend

from ckanext.mongodatastore.controller.storage_controller import VersionedDataStoreController

log = logging.getLogger(__name__)


def raise_exeption(ex):
    raise ex


def create_projection(schema, fields):
    projection = {}
    for field in schema:
        if len(fields) == 0 or field['id'] in fields:
            projection[field['id']] = 1

    return projection


def transform_query_to_statement(query, schema):
    new_filter = {'$or': []}

    for field in schema:
        if field['type'] == "number":
            try:
                new_filter['$or'].append({field['id']: float(query)})
            except TypeError:
                new_filter['$or'].append({field['id']: query})
        else:
            new_filter['$or'].append({field['id']: query})

    return new_filter


def transform_filter(filters, schema):
    new_filter = {}

    schema_dict = {}
    for field in schema:
        schema_dict[field['id']] = field

    for key in filters.keys():
        if type(filters[key]) is list:
            values = []

            if schema_dict[key]['type'] in ['number', 'numeric']:
                for val in filters[key]:
                    try:
                        values.append(float(val))
                    except TypeError:
                        values.append(val)
            else:
                values = filters[key]

            new_filter[key] = {'$in': values}
        else:
            if schema_dict[key]['type'] in ['number', 'numeric']:
                try:

                    if filters[key].startswith('<='):
                        value = float(filters[key][2:])
                        new_filter[key] = {'$lte': value}
                    elif filters[key].startswith('>='):
                        value = float(filters[key][2:])
                        new_filter[key] = {'$gte': value}
                    elif filters[key].startswith('<'):
                        value = float(filters[key][1:])
                        new_filter[key] = {'$lt': value}
                    elif filters[key].startswith('>'):
                        value = float(filters[key][1:])
                        new_filter[key] = {'$gt': value}
                    else:
                        new_filter[key] = float(filters[key])

                except TypeError:
                    new_filter[key] = filters[key]
            else:
                new_filter[key] = filters[key]
    return new_filter


def log_parameter_not_used_warning(param_list):
    for param in param_list:
        if param[1]:
            log.warn('The parameter {0} is set, but has no effect in this DataStore backend implementation!'.format(
                param[0]))


class MongoDataStoreBackend(DatastoreBackend):
    def __init__(self):
        self.mongo_cntr = VersionedDataStoreController.get_instance()
        self.enable_sql_search = True

    def configure(self, cfg):
        VersionedDataStoreController.reload_config(cfg)
        return cfg

    def create(self, context, data_dict):
        log.debug('mongo datastore is created with parameters: {0}'.format(data_dict))

        # Parameters specified by the Datastore API interface
        resource_id = data_dict.get('resource_id', None)
        records = data_dict.get('records', None)
        force = data_dict.get('force', False)
        resource = data_dict.get('resource', None)
        aliases = data_dict.get('aliases', None)
        fields = data_dict.get('fields', None)
        primary_key = data_dict.get('primary_key', 'id')
        indexes = data_dict.get('indexes', None)
        triggers = data_dict.get('triggers', None)
        calculate_record_count = data_dict.get('calculate_record_count', False)

        log_parameter_not_used_warning(
            [('force', force), ('resource', resource), ('aliases', aliases), ('indexes', indexes),
             ('triggers', triggers), ('calculate_record_count', calculate_record_count)])

        if fields:
            log.debug('following fields were passed to this function call: ')
            for field in fields:
                log.debug(field)

        self.mongo_cntr.update_schema(resource_id, fields)

        if primary_key:
            log.debug('The primary key for this resource is {0}'.format(primary_key))
        else:
            log.debug('no primary key was set')

        self.mongo_cntr.create_resource(resource_id, primary_key)

        if records:
            self.mongo_cntr.upsert(resource_id, records)

        return data_dict

    def upsert(self, context, data_dict):
        log.debug('mongo datastore is updated with parameters: {0}'.format(data_dict))

        # Parameters specified by the Datastore API interface
        resource_id = data_dict.get(u'resource_id')
        force = data_dict.get(u'force', False)
        records = data_dict.get(u'records')
        method = data_dict.get(u'method', 'upsert')
        calculate_record_count = data_dict.get(u'calculate_record_count', False)
        dry_run = data_dict.get(u'dry_run', False)

        log_parameter_not_used_warning(
            [('force', force), ('calculate_record_count', calculate_record_count)])

        operations = {
            'insert': lambda a, b, c: raise_exeption(NotImplementedError()),
            'upsert': self.mongo_cntr.upsert,
            'update': lambda a, b, c: raise_exeption(NotImplementedError())
        }

        upsert_operation = operations[method]
        upsert_operation(resource_id, records, dry_run)

        return data_dict

    def delete(self, context, data_dict):
        log.debug('mongo datastore is deleted with parameters: {0}'.format(data_dict))

        # Parameters specified by the Datastore API interface
        resource_id = data_dict.get(u'resource_id')
        force = data_dict.get(u'force', False)
        filters = data_dict.get('filters', {})
        calculate_record_count = data_dict.get(u'calculate_record_count', False)

        log_parameter_not_used_warning(
            [('force', force), ('calculate_record_count', calculate_record_count)])

        self.mongo_cntr.delete_resource(resource_id, filters, force=force)

        return data_dict

    def search(self, context, data_dict):
        log.debug(u'search is performed on mongo datastore with parameters: {0}'.format(data_dict))

        # Parameters specified by the Datastore API interface
        resource_id = data_dict.get(u'resource_id')
        filters = data_dict.get(u'filters', {})
        query = data_dict.get(u'q', None)
        distinct = data_dict.get(u'distinct', False)
        plain = data_dict.get(u'plain', True)
        language = data_dict.get(u'language', u'english')
        limit = data_dict.get(u'limit', 100)
        offset = data_dict.get(u'offset', 0)
        fields = data_dict.get(u'fields', [])
        sort = data_dict.get(u'sort', None)
        include_total = data_dict.get(u'include_total', True)
        total_estimation_threshold = data_dict.get(u'total_estimation_threshold', None)
        records_format = data_dict.get(u'records_format', u'objects')

        log_parameter_not_used_warning([(u'plain', plain), (u'language', language),
                                        (u'total_estimation_threshold', total_estimation_threshold)])

        if records_format in [u'tsv', u'lists']:
            abort(501, u"Unfortunately the current version of MongoDatastore only supports CSV exports!")

        schema = self.resource_fields(data_dict[u'resource_id'])[u'schema']

        if type(fields) is not list:
            fields = fields.split(',')
        projection = create_projection(schema, fields)

        projected_schema = [field for field in schema if field[u'id'] in projection.keys()]

        if query:
            statement = transform_query_to_statement(query, schema)
        else:
            statement = transform_filter(filters, schema)

        if sort:
            splitted_sort_arg = sort.split(' ')
            log.debug(splitted_sort_arg)
            if len(splitted_sort_arg) == 1 or splitted_sort_arg[1] == u'asc':
                sort = [{splitted_sort_arg[0]: 1}]
            else:
                sort = [{splitted_sort_arg[0]: -1}]

        result = self.mongo_cntr.query_current_state(resource_id, statement, projection, sort, offset, limit, distinct,
                                                     include_total, projected_schema, records_format)

        assert ('fields' in result.keys())

        log.debug('id {0} assigned to currently executed search operation.'.format(result['pid']))
        return result

    def search_sql(self, context, data_dict):
        sql_query = data_dict.get(u'sql')

        decoder = JSONDecoder(object_pairs_hook=OrderedDict)

        statement = decoder.decode(sql_query)

        resource_id = data_dict.get(u'resource_id')
        distinct = data_dict.get(u'distinct', False)
        limit = data_dict.get(u'limit', 100)
        offset = data_dict.get(u'offset', 0)
        sort = data_dict.get(u'sort', None)
        include_total = data_dict.get(u'include_total', True)
        records_format = data_dict.get(u'records_format', u'objects')

        if offset:
            offset = int(offset)
        if limit:
            limit = int(limit)

        result = self.mongo_cntr.query_current_state(resource_id, statement, None, sort, offset, limit, distinct,
                                                     include_total, None, records_format)

        return result

    def resource_exists(self, id):
        log.debug(u'resource exists is called on mongo datastore with parameter: {0}'.format(id))
        resource_exists = self.mongo_cntr.resource_exists(id)
        log.debug(u'resource {0} exists: {0}'.format(resource_exists))
        resource_info = get_action('resource_show')(None, {'id': id})
        return self.mongo_cntr.resource_exists(id) and resource_info['datastore_active']

    def resource_fields(self, resource_id):
        return self.mongo_cntr.resource_fields(resource_id)

    def resource_info(self, resource_id):
        return self.resource_fields(resource_id)

    def resource_id_from_alias(self, alias):
        if self.resource_exists(alias):
            return True, alias
        return False, alias

    def get_all_ids(self):
        return self.mongo_cntr.get_all_ids()

    def create_function(self, *args, **kwargs):
        raise NotImplementedError()

    def drop_function(self, *args, **kwargs):
        raise NotImplementedError()