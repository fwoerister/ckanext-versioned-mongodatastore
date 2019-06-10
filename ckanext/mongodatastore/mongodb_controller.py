import csv
import json
import logging
from StringIO import StringIO
from collections import OrderedDict
from json import JSONEncoder

from bson import ObjectId
from ckan.common import config
from pymongo import MongoClient

from ckanext.mongodatastore.helper import normalize_json, calculate_hash, HASH_ALGORITHM
from ckanext.mongodatastore.query_store import QueryStore

log = logging.getLogger(__name__)

type_conversion_dict = {
    'string': str,
    'str': str,
    'char': str,
    'integer': int,
    'int': int,
    'float': float,
    'number': float,

}

CKAN_DATASTORE = config.get(u'ckan.datastore.database')

class MongoDbControllerException(Exception):
    pass


class IdMismatch(MongoDbControllerException):
    pass


class QueryNotFoundException(MongoDbControllerException):
    pass


def convert_to_csv(result_set, fields):
    output = StringIO()
    writer = csv.writer(output, delimiter=';', quotechar='"')

    for record in result_set:
        values = [record.get(key, None) for key in fields]
        writer.writerow(values)

    returnval = output.getvalue()
    output.close()
    return returnval


def create_history_stage(fields, timestamp, id_key='id'):
    group_expression = OrderedDict()
    group_expression['_id'] = '${0}'.format(id_key)

    for field in fields:
        if field not in ['_id']:
            log.debug(str(field))
            if 'info' in field.keys() and field['info'] is not None and 'type_override' in field['info'].keys():
                type_override = field['info']['type_override']

                if type_override == 'float':
                    group_expression[field['id']] = {'$last': {'$convert': {'input': '${0}'.format(field['id']),
                                                                            'to': 'double',
                                                                            'onError': '${0}'.format(field['id'])}}}
                elif type_override == 'string':
                    group_expression[field['id']] = {'$last': {'$convert': {'input': '${0}'.format(field['id']),
                                                                            'to': 'string',
                                                                            'onError': '${0}'.format(field['id'])}}}
                elif type_override == 'int':
                    group_expression[field['id']] = {'$last': {'$convert': {'input': '${0}'.format(field['id']),
                                                                            'to': 'int',
                                                                            'onError': '${0}'.format(field['id'])}}}
                else:
                    group_expression[field['id']] = {'$last': '${0}'.format(field['id'])}
            else:
                group_expression[field['id']] = {'$last': '${0}'.format(field['id'])}

    group_expression['_deleted'] = {'$last': '$_deleted'}

    history_stage = []
    if timestamp:
        history_stage = [
            # remove documents, that were created after the timestamp
            {'$match': {'_id': {'$lte': ObjectId(timestamp)}}}]

    # get the latest versions of each id
    history_stage.append({'$group': group_expression})
    # remove documents, that have already been deleted
    history_stage.append({'$match': {'_deleted': {'$not': {'$eq': True}}}})
    # remove history related attributes
    history_stage.append({'$project': {'_id': 0, '_deleted': 0}})

    return history_stage


class MongoDbController:
    def __init__(self):
        pass

    instance = None

    class __MongoDbController:
        def __init__(self, client, datastore_db, querystore, rows_max):
            self.client = client
            self.datastore = self.client.get_database(datastore_db)
            self.querystore = querystore
            self.rows_max = rows_max

        def __get_collections(self, resource_id):
            col = self.datastore.get_collection(resource_id)
            meta = self.datastore.get_collection('{0}_meta'.format(resource_id))
            fields = self.datastore.get_collection('{0}_fields'.format(resource_id))
            return col, meta, fields

        def __get_max_id(self, resource_id):
            col, _, _ = self.__get_collections(resource_id)

            max_id_results = list(col.aggregate([{'$group': {'_id': '', 'max_id': {'$max': '$_id'}}}], allowDiskUse=True))

            if len(max_id_results) == 0:
                return None
            else:
                return max_id_results[0]['max_id']

        def __update_required(self, resource_id, new_record, id_key):
            col, meta, _ = self.__get_collections(resource_id)

            old_record = col.find_one({id_key: new_record[id_key]}, {'_id': 0})

            if old_record:
                return old_record != new_record
            return True

        def get_all_ids(self):
            return [name for name in self.datastore.list_collection_names() if not name.endswith('_meta')]

        def resource_exists(self, resource_id):
            return resource_id in self.datastore.list_collection_names()

        def create_resource(self, resource_id, primary_key):
            if resource_id not in self.datastore.list_collection_names():
                self.datastore.create_collection(resource_id)
                self.datastore.create_collection('{0}_meta'.format(resource_id))

            self.datastore.get_collection('{0}_meta'.format(resource_id)).insert_one({'record_id': primary_key})

        def delete_resource(self, resource_id, filters={}, force=False):
            if force:
                self.client.get_database(CKAN_DATASTORE).drop_collection(resource_id)
            else:
                col = self.client.get_database(CKAN_DATASTORE).get_collection(resource_id)

                ids_to_delete = col.find(filters, {'_id': 0, 'id': 1})

                for id_to_delete in ids_to_delete:
                    col.insert_one({'id': id_to_delete['id'], '_deleted': True})

        def update_schema(self, resource_id, field_definitions):
            collection, _, fields = self.__get_collections(resource_id)
            fields.delete_many({})
            fields.insert_many(field_definitions)

            for field in field_definitions:
                field.pop('_id')

        def upsert(self, resource_id, records, dry_run=False):
            col, meta, _ = self.__get_collections(resource_id)

            record_id_key = meta.find_one()['record_id']

            records_without_id = [record for record in records if record_id_key not in record.keys()]

            if len(records_without_id) > 0:
                raise MongoDbControllerException('For a datastore upsert, an id '
                                                 'value has to be set for every record. '
                                                 'In this collection the id attribute is "{0}"'.format(record_id_key))

            for record in records:
                if not dry_run:
                    if self.__update_required(resource_id, record, record_id_key):
                        col.insert_one(record)
                        record.pop('_id')

        def retrieve_stored_query(self, pid, offset, limit, records_format='objects'):
            q = self.querystore.retrieve_query(pid)

            if q:
                pipeline = json.JSONDecoder(object_pairs_hook=OrderedDict).decode(q.query)

                result = self.__query(q.resource_id,
                                      pipeline,
                                      ObjectId(q.timestamp),
                                      offset,
                                      limit,
                                      True)

                result['pid'] = pid
                result['query'] = q

                query = json.JSONDecoder(object_pairs_hook=OrderedDict).decode(q.query)
                projection = [projection for projection in query if '$project' in projection.keys()][-1]['$project']

                stored_field_info = list(self.resource_fields(q.resource_id, q.timestamp)['schema'])

                if projection and len([k for k in projection.keys() if projection[k] != 0]) != 0:
                    fields = [field for field in stored_field_info if field['id'] in projection.keys()]
                else:
                    fields = stored_field_info

                field_names = [field['id'] for field in fields]

                result['fields'] = fields

                if records_format == 'objects':
                    result['records'] = list(result['records'])
                elif records_format == 'csv':

                    log.debug(fields)
                    log.debug(projection)
                    log.debug(stored_field_info)

                    result['records'] = convert_to_csv(result['records'], field_names)

                return result
            else:
                raise QueryNotFoundException('Unfortunately there is no query stored with PID {0}'.format(pid))

        # TODO: refactor field generation (duplicate code in query_current_state and retrieve_stored_query)
        def query_current_state(self, resource_id, statement, projection, sort, offset, limit, distinct, include_total,
                                records_format='objects'):

            timestamp = self.__get_max_id(resource_id)
            field_timestamp = self.__get_max_id('{0}_fields'.format(resource_id))

            if field_timestamp > timestamp:
                timestamp = field_timestamp

            if sort is None:
                sort = [{'id': 1}]
            else:
                sort = sort + [{'id': 1}]

            sort_dict = OrderedDict()
            for sort_entry in sort:
                assert (len(sort_entry.keys()) == 1)
                sort_dict[sort_entry.keys()[0]] = sort_entry[sort_entry.keys()[0]]

            statement = normalize_json(statement)
            if projection:
                projection = normalize_json(projection)

            pipeline = [
                {'$match': statement},
                {'$sort': sort_dict}
            ]

            if distinct:
                group_expr = {'$group': {'_id': {}}}
                sort_dict = OrderedDict()
                for field in projection.keys():
                    if field != '_id':
                        group_expr['$group']['_id'][field] = '${0}'.format(field)
                        group_expr['$group'][field] = {'$first': '${0}'.format(field)}
                        sort_dict[field] = 1
                sort_dict['id'] = 1

                pipeline.append(group_expr)
                pipeline.append({'$sort': sort_dict})

            if projection:
                pipeline.append({'$project': projection})

            result = self.__query(resource_id, pipeline, timestamp, offset, limit, include_total)

            pid = self.querystore.store_query(resource_id, result['query'], str(timestamp),
                                              result['records_hash'], result['query_hash'], HASH_ALGORITHM().name)

            result['pid'] = pid

            stored_field_info = self.resource_fields(resource_id, timestamp)['schema']

            if projection:
                fields = [field for field in stored_field_info if field['id'] in projection.keys()]
            else:
                fields = stored_field_info

            field_names = [field['id'] for field in fields]

            result['fields'] = fields

            if records_format == 'objects':
                result['records'] = list(result['records'])
            elif records_format == 'csv':
                result['records'] = convert_to_csv(result['records'], field_names)

            return result

        def __query(self, resource_id, pipeline, timestamp, offset, limit, include_total):
            col, meta, _ = self.__get_collections(resource_id)

            resource_fields = self.resource_fields(resource_id, timestamp)
            schema = resource_fields['schema']
            id_key = resource_fields['meta']['record_id']
            field_ids = [field['id'] for field in schema]

            history_stage = create_history_stage(schema, timestamp, id_key)
            pagination_stage = []

            if offset and offset > 0:
                pagination_stage.append({'$skip': offset})

            if limit:
                if 0 < limit <= self.rows_max:
                    pagination_stage.append({'$limit': limit})
                if limit < self.rows_max:
                    pagination_stage.append({'$limit': self.rows_max})
                    limit = self.rows_max

            resultset_hash = calculate_hash(col.aggregate(history_stage + pipeline, allowDiskUse=True))

            if include_total:
                count = list(col.aggregate(history_stage + pipeline + [{u'$count': u'count'}], allowDiskUse=True))

                if len(count) == 0:
                    count = 0
                else:
                    count = count[0]['count']

            query = JSONEncoder().encode(pipeline)

            records = col.aggregate(history_stage + pipeline + pagination_stage, allowDiskUse=True)

            query_hash = calculate_hash(query)

            result = {'records': records,
                      'records_hash': resultset_hash,
                      'query': query,
                      'query_hash': query_hash}

            if include_total:
                result['total'] = count

            if limit:
                result['limit'] = limit
            if offset:
                result['offset'] = offset

            return result

        def resource_fields(self, resource_id, timestamp=None, show_all=False):
            col, meta, fields = self.__get_collections(resource_id)

            meta_entry = meta.find_one()
            schema = fields.find({}, {'_id': 0})

            return {'meta': meta_entry, 'schema': list(schema)}

    @classmethod
    def getInstance(cls):
        if MongoDbController.instance is None:
            client = MongoClient(config.get(u'ckan.datastore.write_url'))
            querystore = QueryStore(config.get(u'ckan.querystore.url'))
            rows_max = config.get(u'ckan.datastore.search.rows_max', 100)
            MongoDbController.instance = MongoDbController.__MongoDbController(client, config.get(u'ckan.datastore.database'), querystore,
                                                                               rows_max)
        return MongoDbController.instance

    @classmethod
    def reloadConfig(cls, cfg):
        client = MongoClient(cfg.get(u'ckan.datastore.write_url'))
        querystore = QueryStore(cfg.get(u'ckan.querystore.url'))
        rows_max = config.get(u'ckan.datastore.search.rows_max', 100)
        MongoDbController.instance = MongoDbController.__MongoDbController(client, config.get(u'ckan.datastore.database'), querystore, rows_max)
