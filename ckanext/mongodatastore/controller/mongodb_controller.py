import json
import logging
from datetime import datetime

import pymongo
import pytz
from ckan.common import config
from ckan.plugins import toolkit
from pymongo import MongoClient
from pymongo.collation import Collation

from ckanext.mongodatastore.controller.querystore_controller import QueryStoreController
from ckanext.mongodatastore.exceptions import MongoDbControllerException, QueryNotFoundException
from ckanext.mongodatastore.query_preprocessor import transform_query_to_statement, transform_filter, create_projection, \
    transform_sort
from ckanext.mongodatastore.util import normalize_json, DateTimeEncoder, HASH_ALGORITHM, decodeDateTime, \
    calculate_hash

log = logging.getLogger(__name__)
type_conversion_dict = {
    'string': str,
    'str': str,
    'char': str,
    'integer': int,
    'int': int,
    'float': float,
    'number': float,
    'numeric': float,
}
CKAN_DATASTORE = config.get(u'ckan.datastore.database')
CKAN_SITE_URL = config.get(u'ckan.site_url')


def calculate_resultset_hash_job(pid):
    client = MongoClient(config.get(u'ckan.datastore.write_url'))
    querystore = QueryStoreController(config.get(u'ckan.querystore.url'))

    q, metadata = querystore.retrieve_query(pid)
    c = client.get_database(config.get(u'ckan.datastore.database')).get_collection(q.resource_id)
    parsed_query = json.loads(q.query, object_hook=decodeDateTime)

    log.info("fetch {0}".format(parsed_query['filter']))
    result = c.find(filter=parsed_query['filter'], projection=parsed_query['projection'], sort=parsed_query['sort'])

    _hash = calculate_hash(result)
    querystore.update_hash(pid, _hash)


class VersionedDataStoreController:
    def __init__(self):
        pass

    instance = None

    class __VersionedDataStoreController:
        def __init__(self, client, datastore_db, querystore, rows_max):
            self.client = client
            self.datastore = self.client.get_database(datastore_db)
            self.querystore = querystore
            self.rows_max = rows_max

        @staticmethod
        def _execute_query(col, distinct, limit, offset, projected_schema, projection, sort, statement):
            if distinct:
                return [{projected_schema[0]['id']: val} for val in
                        col.distinct(projected_schema[0]['id'])]
            else:
                curs = col.find(filter=statement, projection=projection, skip=offset,
                                limit=limit)
                if sort:
                    return curs.sort(sort)
                return curs

        @staticmethod
        def _prepare_projection(projection):
            if projection and 1 in projection.values():
                projection.update({'_id': 0})
            else:
                projection = {'_id': 0, '_created': 0, '_valid_to': 0, '_latest': 0}

            return normalize_json(projection)

        @staticmethod
        def __apply_override_type(records, fields):
            field_type = dict()

            for field in fields.find():
                if 'info' in field and 'type_override' in field['info']:
                    if field['info']['type_override'] in type_conversion_dict:
                        field_type[field['id']] = type_conversion_dict[field['info']['type_override']]
                    else:
                        field_type[field['id']] = type_conversion_dict[field['type']]

            for record in records:
                for field in record.keys():
                    if record[field]:
                        if field_type[field] in [float, int] and (record[field] == '' or str(record[field]).isspace()):
                            record[field] = None
                        else:
                            try:
                                record[field] = field_type[field](record[field])
                            except ValueError as e:
                                print(e)

        def __get_collections(self, resource_id):
            col = self.datastore.get_collection(resource_id)
            meta = self.datastore.get_collection('{0}_meta'.format(resource_id))
            fields = self.datastore.get_collection('{0}_fields'.format(resource_id))
            return col, meta, fields

        def __update_required(self, resource_id, new_record, id_key):
            col, meta, _ = self.__get_collections(resource_id)

            result = col.find_one(
                {'_latest': True, '_hash': new_record['_hash'], id_key: new_record[id_key]}, {'_hash': 1})
            return result is None

        def get_all_ids(self):
            return [name for name in self.datastore.list_collection_names() if
                    not (name.endswith('_meta') or name.endswith('_fields'))]

        def resource_exists(self, resource_id):
            return resource_id in self.datastore.list_collection_names()

        def create_resource(self, resource_id, primary_key):
            if resource_id not in self.datastore.list_collection_names():
                self.datastore.create_collection(resource_id)
                self.datastore.create_collection('{0}_meta'.format(resource_id))

            self.client.admin.command('shardCollection', 'CKAN_Datastore.{0}'.format(resource_id),
                                      key={primary_key: 'hashed'})

            self.datastore.get_collection('{0}_meta'.format(resource_id)).insert_one(
                {'record_id': primary_key, 'active': True})

            col = self.datastore.get_collection(resource_id)

            col.create_index(
                [('_created', pymongo.ASCENDING), ('_valid_to', pymongo.DESCENDING), ('_id', pymongo.ASCENDING)],
                name='_created_valid_to_index')

            col.create_index([('_latest', pymongo.DESCENDING), (primary_key, pymongo.ASCENDING)],
                             name='_valid_to_pk_index')

            col.create_index([(primary_key, pymongo.ASCENDING)],
                             name='_id_index')

        def delete_resource(self, resource_id, filters={}):
            col = self.client.get_database(CKAN_DATASTORE).get_collection(resource_id)
            filters['_valid_to'] = {'$exists': False}
            col.update_many(filters, {'$currentDate': {'_valid_to': True}, '$set': {'_latest': False}})

        def update_schema(self, resource_id, field_definitions, indexes, primary_key):
            collection, _, fields = self.__get_collections(resource_id)
            fields.delete_many({})
            fields.insert_many(field_definitions)

            type_dict = {}
            text_indices = []

            for field in field_definitions:
                if field['type'] == 'text':
                    text_indices.append(field['id'])
                    collection.create_index([(field['id'], 1)], collation=Collation(locale='en'))
                type_dict[field['id']] = field['type']

            if indexes:
                for index in indexes:
                    if index != primary_key and index not in text_indices:
                        collection.create_index([(index, pymongo.ASCENDING), ('_created', pymongo.DESCENDING),
                                                 ('_latest', pymongo.DESCENDING)],
                                                name='{0}_index'.format(index))
            for field in field_definitions:
                field.pop('_id')

        def insert(self, resource_id, records, dry_run=False):
            col, meta, fields = self.__get_collections(resource_id)
            record_id_key = meta.find_one()['record_id']

            self.__apply_override_type(records, fields)

            records_without_id = [record for record in records if record_id_key not in record.keys()]

            if len(records_without_id) > 0:
                raise MongoDbControllerException('For a datastore upsert, an id '
                                                 'value has to be set for every record. '
                                                 'In this collection the id attribute is "{0}"'.format(record_id_key))

            if not dry_run:
                for record in records:
                    record['_hash'] = calculate_hash(record)
                    col.update_one({record_id_key: record[record_id_key], '_latest': True},
                                   {'$currentDate': '_created',
                                    '$set': record}, upsert=True)

        def upsert(self, resource_id, records, dry_run=False):
            col, meta, fields = self.__get_collections(resource_id)
            record_id_key = meta.find_one()['record_id']

            self.__apply_override_type(records, fields)

            records_without_id = [record for record in records if record_id_key not in record.keys()]

            if len(records_without_id) > 0:
                raise MongoDbControllerException('For a datastore upsert, an id '
                                                 'value has to be set for every record. '
                                                 'In this collection the id attribute is "{0}"'.format(record_id_key))

            for record in records:
                if not dry_run:
                    record['_hash'] = calculate_hash(record)
                    if self.__update_required(resource_id, record, record_id_key):
                        filters = {'_latest': True, record_id_key: record[record_id_key]}
                        col.update_one(filters, {'$currentDate': {'_valid_to': True}, '$set': {'_latest': False}})
                        record['_latest'] = True
                        record['_created'] = datetime.now(pytz.UTC)
                        col.insert_one(record)

        def issue_pid(self, resource_id, statement, projection, sort, distinct, q):
            now = datetime.now(pytz.UTC)

            col, meta, fields = self.__get_collections(resource_id)
            result = dict()
            schema = fields.find()

            if q:
                statement = transform_query_to_statement(q, schema)
            else:
                statement = transform_filter(statement, schema)

            statement = normalize_json(statement)

            statement.update({'_valid_to': {'$gt': now},
                              '_created': {'$lte': now}
                              })

            projection = create_projection(fields.find(), projection)

            if sort:
                sort = transform_sort(sort)
            else:
                sort = [('_created', 1), ('_id', 1)]

            projected_schema = [field for field in fields.find() if field[u'id'] in projection.keys()]

            query, meta_data = self.querystore.store_query(resource_id,
                                                           json.dumps({'filter': statement,
                                                                       'projection': projection,
                                                                       'sort': sort}, cls=DateTimeEncoder),
                                                           str(now),
                                                           None, HASH_ALGORITHM().name,
                                                           projected_schema)

            toolkit.enqueue_job(calculate_resultset_hash_job, [query.id])

            result['metadata'] = meta_data
            result['query'] = query
            fields = []
            for field in query.record_fields:
                fields.append({
                    'id': field.name,
                    'type': field.datatype,
                    'info': {
                        'description': field.description
                    }
                })
            result['fields'] = fields
            return query.id

        def execute_stored_query(self, pid, offset, limit, preview=False):
            q, metadata = self.querystore.retrieve_query(pid)

            if q:
                col, meta, _ = self.__get_collections(q.resource_id)
                parsed_query = json.loads(q.query, object_hook=decodeDateTime)
                result = dict()

                result['pid'] = pid

                if preview:
                    result['records'] = col.find(filter=parsed_query.get('filter'),
                                                 projection=parsed_query.get('projection'),
                                                 sort=parsed_query.get('sort'),
                                                 skip=offset,
                                                 limit=limit,
                                                 hint='_created_valid_to_index')

                query = {
                    'id': q.id,
                    'resource_id': q.resource_id,
                    'query': q.query,
                    'query_hash': q.query_hash,
                    'hash_algorithm': q.hash_algorithm,
                    'result_set_hash': q.result_set_hash,
                    'timestamp': q.timestamp,
                    'handle_pid': q.handle_pid
                }
                result['query'] = query

                fields = []
                field_names = []
                for field in q.record_fields:
                    field_names.append(field.name)
                    fields.append({
                        'id': field.name,
                        'type': field.datatype,
                        'info': {
                            'description': field.description
                        }
                    })

                result['fields'] = fields
                result['meta'] = metadata

                return result
            else:
                raise QueryNotFoundException('No query with PID {0} found'.format(pid))

        def query_current_state(self, resource_id, statement, projection, sort, offset, limit, distinct, include_total,
                                projected_schema, unversioned=False):
            col, _, _ = self.__get_collections(resource_id)
            result = dict()

            get_full_collection = statement == {}

            if get_full_collection:
                statement = {'_latest': True}
            else:
                statement = {
                    '$and': [
                        {'_latest': True},
                        statement
                    ]
                }

            if sort is None:
                sort = [('_id', 1)]

            if include_total:
                if get_full_collection:
                    result['total'] = col.count_documents({'_latest': True})
                else:
                    result['total'] = col.count_documents(statement)

            projection = self._prepare_projection(projection)

            res = self._execute_query(col, distinct, limit, offset, projected_schema, projection, sort,
                                      statement)

            result['records'] = list(res)

            if type(res) != list:
                res.close()

            return result

        def resource_fields(self, resource_id):
            col, meta, fields = self.__get_collections(resource_id)

            meta_entry = meta.find_one({}, {"_id": 0})
            schema = fields.find({}, {'_id': 0})

            return {'meta': meta_entry, 'schema': list(schema)}

    @classmethod
    def get_instance(cls):
        if VersionedDataStoreController.instance is None:
            log.info(config.get(u'ckan.datastore.write_url'))
            client = MongoClient(config.get(u'ckan.datastore.write_url'))
            client.admin.command('enableSharding', 'CKAN_Datastore')

            querystore = QueryStoreController(config.get(u'ckan.querystore.url'))
            rows_max = config.get(u'ckan.datastore.search.rows_max', 20)
            cls.instance = VersionedDataStoreController.__VersionedDataStoreController(client,
                                                                                       config.get(
                                                                                           u'ckan.datastore.database'),
                                                                                       querystore,
                                                                                       rows_max)
        return VersionedDataStoreController.instance

    @classmethod
    def reload_config(cls, cfg):
        client = MongoClient(cfg.get(u'ckan.datastore.write_url'))
        querystore = QueryStoreController(cfg.get(u'ckan.querystore.url'))
        rows_max = config.get(u'ckan.datastore.search.rows_max', 100)
        VersionedDataStoreController.instance = VersionedDataStoreController.__VersionedDataStoreController(client,
                                                                                                            config.get(
                                                                                                                u'ckan.datastore.database'),
                                                                                                            querystore,
                                                                                                            rows_max)
