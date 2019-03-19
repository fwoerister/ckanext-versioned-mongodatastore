import csv
import json
import logging
from StringIO import StringIO
from collections import OrderedDict
from json import JSONEncoder

from bson import ObjectId
from ckan.common import config
from pymongo import MongoClient

from ckanext.mongodatastore.helper import normalize_json, CKAN_DATASTORE, calculate_hash, HASH_ALGORITHM
from ckanext.mongodatastore.query_store import QueryStore

log = logging.getLogger(__name__)


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
            return list(col.aggregate([{'$group': {'_id': '', 'max_id': {'$max': '$_id'}}}]))[0]['max_id']

        def __generate_history_group_expression(self, resource_id, timestamp):
            expression = OrderedDict()
            expression['_id'] = '$id'

            fields = self.resource_fields(resource_id, timestamp)
            fields = fields['schema']

            for key in fields:
                if key not in ['_id']:
                    expression[key] = {'$last': '${0}'.format(key)}

            expression['_deleted'] = {'$last': '$_deleted'}
            return expression

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

        def update_datatypes(self, resource_id, fields):
            col, meta, field_collection = self.__get_collections(resource_id)

            timestamp = self.__get_max_id(resource_id)

            pipeline = [{'$match': {}}]

            result = self.__query(resource_id, pipeline, timestamp, None, None, False)

            meta_record = meta.find_one()
            record_id = meta_record['record_id']

            converter = {
                'text': str,
                'string': str,
                'numeric': float,
                'number': float
            }

            override_fields = [{'id': field['id'], 'new_type': field['info']['type_override']} for field in fields if
                               len(field['info']['type_override']) > 0]
            for record in result['records']:
                for field in override_fields:
                    try:
                        record[field['id']] = converter[field['new_type']](record[field['id']])
                    except ValueError:
                        log.warn('Could not convert field {0} of record {1} in resource {2}'.format(field['id'],
                                                                                                    record[record_id],
                                                                                                    resource_id))
                self.upsert(resource_id, [record], False)

            for field in fields:
                field_collection.insert_one(field)

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

                schema = self.resource_fields(q.resource_id, q.timestamp)['schema']

                printable_fields = []

                for field in schema:
                    if not projection or projection.get(field, 0) == 1:
                        printable_fields.append(field)

                if len(printable_fields) == 0:
                    printable_fields = schema.keys()

                if records_format == 'csv':
                    result['records'] = convert_to_csv(result['records'], printable_fields)
                else:
                    result['records'] = list(result['records'])

                schema_fields = []
                for field in schema.keys():
                    log.debug(field)
                    if not projection or (field in projection and projection[field] == 1):
                        schema_fields.append({'id': field, 'type': schema[field]})

                result['fields'] = schema_fields

                log.debug('schema fields {0}'.format(schema_fields))

                return result
            else:
                raise QueryNotFoundException('Unfortunately there is no query stored with PID {0}'.format(pid))

        # TODO: refactor field generation (duplicate code in query_current_state and retrieve_stored_query)
        def query_current_state(self, resource_id, statement, projection, sort, offset, limit, distinct, include_total,
                                records_format='objects'):

            timestamp = self.__get_max_id(resource_id)

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

                log.debug('$group stage: {0}'.format(group_expr))
                pipeline.append(group_expr)
                pipeline.append({'$sort': sort_dict})

            if projection:
                log.debug('projection: {0}'.format(projection))
                pipeline.append({'$project': projection})

            result = self.__query(resource_id, pipeline, timestamp, offset, limit, include_total)

            pid = self.querystore.store_query(resource_id, result['query'], str(timestamp),
                                              result['records_hash'], result['query_hash'], HASH_ALGORITHM().name)

            result['pid'] = pid

            schema = self.resource_fields(resource_id, timestamp)['schema']

            printable_fields = []

            for field in schema:
                if not projection or projection.get(field, 0) == 1:
                    printable_fields.append(field)

            if len(printable_fields) == 0:
                printable_fields = schema.keys()

            if records_format == 'objects':
                result['records'] = list(result['records'])
            elif records_format == 'csv':
                result['records'] = convert_to_csv(result['records'], printable_fields)

            schema_fields = []
            for field in schema.keys():
                log.debug(field)
                if not projection or (field in projection and projection[field] == 1):
                    schema_fields.append({'id': field, 'type': schema[field]})

            result['fields'] = schema_fields

            return result

        def __query(self, resource_id, pipeline, timestamp, offset, limit, include_total):
            col, meta, _ = self.__get_collections(resource_id)

            history_stage = [
                # remove documents, that were created after the timestamp
                {'$match': {'_id': {'$lte': timestamp}}},

                # get the latest versions of each id
                {'$group': self.__generate_history_group_expression(resource_id, timestamp)},

                # remove documents, that have already been deleted
                {'$match': {'_deleted': {'$not': {'$eq': True}}}},

                # remove history related attributes
                {'$project': {'_id': 0, '_deleted': 0}}
            ]

            pagination_stage = []

            if offset and offset > 0:
                pagination_stage.append({'$skip': offset})

            if limit:
                if 0 < limit <= self.rows_max:
                    pagination_stage.append({'$limit': limit})
                if limit < self.rows_max:
                    pagination_stage.append({'$limit': self.rows_max})
                    limit = self.rows_max

            resultset_hash = calculate_hash(col.aggregate(history_stage + pipeline))

            if include_total:
                count = list(col.aggregate(history_stage + pipeline + [{u'$count': u'count'}]))

                if len(count) == 0:
                    count = 0
                else:
                    count = count[0]['count']

            query = JSONEncoder().encode(pipeline)

            log.debug('submitted query: {0}'.format(history_stage + pipeline + pagination_stage))
            records = col.aggregate(history_stage + pipeline + pagination_stage)

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

        def resource_fields(self, resource_id, timestamp=None):
            col, meta, _ = self.__get_collections(resource_id)

            pipeline = []

            if timestamp:
                pipeline = [
                    {'$match': {'_id': {'$lte': ObjectId(timestamp)}}}
                ]
            pipeline.append({'$project': {"arrayofkeyvalue": {'$objectToArray': '$$ROOT'}}})
            pipeline.append({'$unwind': '$arrayofkeyvalue'})
            pipeline.append({'$group': {'_id': None, 'keys': {'$addToSet': '$arrayofkeyvalue.k'}}})

            result = col.aggregate(pipeline)

            schema = OrderedDict()

            result = list(result)

            if len(result) > 0:
                result = result[0]
                for key in sorted(result['keys']):
                    if key not in ['_id', 'valid_to', 'id', '_deleted']:
                        schema[key] = 'string'
                    if key == 'id':
                        schema['id'] = 'number'
            else:
                schema['id'] = 'number'

            result = {u'schema': schema, u'meta': meta.find_one()}

            return result

    @classmethod
    def getInstance(cls):
        if MongoDbController.instance is None:
            client = MongoClient(config.get(u'ckan.datastore.write_url'))
            querystore = QueryStore(config.get(u'ckan.querystore.url'))
            rows_max = config.get(u'ckan.datastore.search.rows_max', 100)
            MongoDbController.instance = MongoDbController.__MongoDbController(client, CKAN_DATASTORE, querystore,
                                                                               rows_max)
        return MongoDbController.instance

    @classmethod
    def reloadConfig(cls, cfg):
        client = MongoClient(cfg.get(u'ckan.datastore.write_url'))
        querystore = QueryStore(cfg.get(u'ckan.querystore.url'))
        rows_max = config.get(u'ckan.datastore.search.rows_max', 100)
        MongoDbController.instance = MongoDbController.__MongoDbController(client, CKAN_DATASTORE, querystore, rows_max)
