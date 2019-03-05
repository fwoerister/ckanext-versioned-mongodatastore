import csv
import json
import logging
from StringIO import StringIO
from collections import OrderedDict
from datetime import datetime

import pytz
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


def convert_to_object_id(datetime_value):
    epoch = datetime(1970, 1, 1, 0, 0, 0, 0, tzinfo=pytz.UTC)
    hex_timestamp = hex(int((datetime_value - epoch).total_seconds()))[2:]
    return ObjectId(hex_timestamp + '0000000000000000')


def generate_group_expression(projection):
    expression = OrderedDict()

    expression['_id'] = '${0}'.format(id_key)

    for key in projection:
        expression[key] = {'$last': '$id'}

    return expression


# TODO: implement session handling + rollbacks in case of failed transactions
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
            return col, meta

        def __is_empty(self, resource_id):
            col, _ = self.__get_collections(resource_id)
            return col.count() == 0

        def get_all_ids(self):
            return [name for name in self.datastore.list_collection_names() if not name.endswith('_meta')]

        def resource_exists(self, resource_id):
            return resource_id in self.datastore.list_collection_names()

        def create_resource(self, resource_id, primary_key):
            if resource_id not in self.datastore.list_collection_names():
                self.datastore.create_collection(resource_id)
                self.datastore.create_collection('{0}_meta'.format(resource_id))

            log.debug('record entry added')
            self.datastore.get_collection('{0}_meta'.format(resource_id)).insert_one({'record_id': primary_key})

        def delete_resource(self, resource_id, filters, force=False):
            if force:
                self.client.get_database(CKAN_DATASTORE).drop_collection(resource_id)
            else:
                col = self.client.get_database(CKAN_DATASTORE).get_collection(resource_id)

                timestamp = convert_to_object_id(datetime.utcnow().replace(tzinfo=pytz.UTC))

                if filters:
                    for record in col.find({'$and': [{'valid_to': {'$exists': 0}}, filters]}):
                        col.update_one({'_id': record['_id']}, {'$set': {'valid_to': timestamp}})
                else:
                    for record in col.find({'valid_to': {'$exists': 0}}):
                        col.update_one({'_id': record['_id']}, {'$set': {'valid_to': timestamp}})

        def update_datatypes(self, resource_id, fields):
            col, meta = self.__get_collections(resource_id)

            # TODO: This is a workaround, as utcnow() does not set the correct timezone!
            timestamp = convert_to_object_id(datetime.utcnow().replace(tzinfo=pytz.UTC))

            pipeline = [{'$match': {'_id': {'$le': timestamp}}}, {'$match': {'$or': [
                {'valid_to': {'$exists': 0}},
                {'valid_to': {'$gt': timestamp}}
            ]}}]

            result = self.__query(resource_id, pipeline, 0, 0, False)

            print(result)

            # print("{0} documents are updated".format(len(list(result['records']))))

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

            print("{0} fields are modified")
            print(override_fields)

            for record in result['records']:
                for field in override_fields:
                    print('{0} - {1}'.format(record['id'], field))
                    try:
                        record[field['id']] = converter[field['new_type']](record[field['id']])

                        print('new value for file {0} is {1}'.format(field['id'], record[field['id']]))

                    except TypeError:
                        print('Could not convert field {0} of record {1} in resource {2}'.format(field['id'],
                                                                                                 record[record_id],
                                                                                                 resource_id))
                        log.warn('Could not convert field {0} of record {1} in resource {2}'.format(field['id'],
                                                                                                    record[record_id],
                                                                                                    resource_id))
                record.pop('_id')
                print('upsert document: {0}'.format(record))
                self.upsert(resource_id, [record], False)
            # TODO: store override information in meta entry

        # TODO: check if record has to be updated at all (in case it did not change, no update has to be performed)
        def upsert(self, resource_id, records, dry_run):
            col, meta = self.__get_collections(resource_id)

            record_id_key = meta.find_one()['record_id']

            records_without_id = [record for record in records if record_id_key not in record.keys()]

            if len(records_without_id) > 0:
                raise MongoDbControllerException('For a datastore upsert, every an id '
                                                 'value has to be set for every record. '
                                                 'In this collection the id attribute is "{0}"'.format(record_id_key))

            for record in records:
                if not dry_run:
                    result = col.insert_one(record)

        def retrieve_stored_query(self, pid, offset, limit, check_integrity=False, records_format='objects'):
            q = self.querystore.retrieve_query(pid)

            if q:
                pipeline = json.JSONDecoder(object_pairs_hook=OrderedDict).decode(q.query)

                result = self.__query(q.resource_id,
                                      pipeline,
                                      offset,
                                      limit,
                                      check_integrity)

                if check_integrity:
                    return result == q.result_set_hash

                result['pid'] = pid
                result['query'] = q

                if records_format == 'csv':
                    query = json.JSONDecoder(object_pairs_hook=OrderedDict).decode(q.query)
                    projection = query[-1]['$project']
                    fields = [field for field in projection if projection[field] == 1]

                    result['records'] = convert_to_csv(result['records'], fields)
                else:
                    result['records'] = list(result['records'])

                return result
            else:
                raise QueryNotFoundException('Unfortunately there is no query stored with PID {0}'.format(pid))

        def query_current_state(self, resource_id, statement, projection, sort, offset, limit, distinct, include_total,
                                records_format='objects', check_integrity=False):

            # TODO: This is a workaround, as utcnow() does not set the correct timezone!
            timestamp = convert_to_object_id(datetime.utcnow().replace(tzinfo=pytz.UTC))

            if sort is None:
                sort = [{'id': 1}]
            else:
                sort = sort + [{'id': 1}]

            projection = normalize_json(projection)
            statement = normalize_json(statement)

            sort_dict = OrderedDict()
            for sort_entry in sort:
                assert (len(sort_entry.keys()) == 1)
                sort_dict[sort_entry.keys()[0]] = sort_entry[sort_entry.keys()[0]]

            statement = normalize_json(statement)
            if projection:
                projection = normalize_json(projection)

            pipeline = [
                {'$match': {'_id': {'$le': timestamp}}},
                {'$match': {'$or': [{'$exists': {'valid_to': 0}}, {'valid_to': {'$gt': timestamp}}]}},
                {'$group': generate_group_expression(projection)},
                {'$match': statement},
                {'$sort': sort}
            ]

            if distinct:
                group_expr = {'$group': {'_id': {}}}
                for field in projection.keys():
                    if field != '_id':
                        group_expr['$group']['_id'][field] = '${0}'.format(field)
                        group_expr['$group'][field] = {'$first': '${0}'.format(field)}

                log.debug('$group stage: {0}'.format(group_expr))
                pipeline.append(group_expr)

            if projection:
                log.debug('projection: {0}'.format(projection))
                pipeline.append({'$project': projection})

            result = self.__query(resource_id, pipeline, offset, limit, include_total, check_integrity)

            pid = self.querystore.store_query(resource_id, result['query'], result['query_with_removed_ts'], timestamp,
                                              result['records_hash'], result[
                                                  'query_hash'], HASH_ALGORITHM().name)

            result['pid'] = pid

            if records_format == 'objects':
                result['records'] = list(result['records'])
            elif records_format == 'csv':
                schema = self.resource_fields(resource_id)['schema']
                fields = [field for field in schema.keys()]
                result['records'] = convert_to_csv(result['records'], fields)

            return result

        def __query(self, resource_id, pipeline, offset, limit, include_total, check_integrity=False):
            col, meta = self.__get_collections(resource_id)

            resultset_hash = calculate_hash(col.aggregate(pipeline))

            if check_integrity:
                return resultset_hash

            if include_total:
                count = list(col.aggregate(pipeline + [{'$count': 'count'}]))

                if len(count) == 0:
                    count = 0
                else:
                    count = count[0]['count']

            query = json.JSONEncoder().encode(pipeline)
            # the timestamps have to be removed, otherwise the querystore would detected a new query every time,
            # as the timestamps within the query change the hash all the time
            query_with_removed_ts = json.JSONEncoder().encode(pipeline[1:])

            if offset and offset > 0:
                pipeline.append({'$skip': offset})

            if limit:
                if 0 < limit <= self.rows_max:
                    pipeline.append({'$limit': limit})
                if limit < self.rows_max:
                    pipeline.append({'$limit': self.rows_max})
                    limit = self.rows_max

            log.debug('final pipeline: {0}'.format(pipeline))
            log.debug('limit: {0}'.format(limit))
            log.debug('rows_max: {0}'.format(self.rows_max))

            log.debug('offset: {0}'.format(offset))
            result = col.aggregate(pipeline)

            projection = [stage['$project'] for stage in pipeline if '$project' in stage.keys()]
            assert (len(projection) <= 1)
            if len(projection) == 1:
                projection = projection[0]
                projection = [field for field in projection if projection[field] == 1]

                schema = self.resource_fields(resource_id)['schema']
                fields = []
                for field in schema.keys():
                    if field in projection:
                        fields.append({'id': field, 'type': schema[field]})
            else:
                fields = []

            query_hash = calculate_hash(query_with_removed_ts)

            result = {'records': result,
                      'fields': fields,
                      'records_hash': resultset_hash,
                      'query': query,
                      'query_with_removed_ts': query_with_removed_ts,
                      'query_hash': query_hash}

            if include_total:
                result['total'] = count

            if limit:
                result['limit'] = limit
            if offset:
                result['offset'] = offset

            return result

        def resource_fields(self, resource_id):
            col, meta = self.__get_collections(resource_id)

            pipeline = [
                {'$match': {'$exists': {'valid_to': 0}}},
                {'$project': {"arrayofkeyvalue": {'$objectToArray': '$$ROOT'}}},
                {'$unwind': '$arrayofkeyvalue'},
                {'$group': {'_id': None, 'keys': {'$addToSet': '$arrayofkeyvalue.k'}}}
            ]

            result = col.aggregate(pipeline)

            # result = col.map_reduce(mapper, reducer, "{0}_keys".format(resource_id))
            schema = OrderedDict()

            for key in sorted(result['keys']):
                schema[key] = 'text'  # TODO: guess data type

            log.debug(meta.find_one())
            return {u'schema': schema, u'meta': meta.find_one()}

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
