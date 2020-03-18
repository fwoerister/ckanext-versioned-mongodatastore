import csv
import json
import logging

import pymongo
from StringIO import StringIO
from collections import OrderedDict
from datetime import datetime
from json import JSONEncoder

import pytz
from bson import ObjectId, json_util
from ckan.logic import get_action

from ckan.common import config
from pyhandle.clientcredentials import PIDClientCredentials
from pyhandle.handleclient import PyHandleClient
from pymongo import MongoClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from ckanext.mongodatastore.exceptions import MongoDbControllerException, QueryNotFoundException
from ckanext.mongodatastore.helper import normalize_json, calculate_hash, HASH_ALGORITHM
from ckanext.mongodatastore.model import Query, RecordField, MetaDataField

log = logging.getLogger(__name__)

type_conversion_dict = {
    'string': str,
    'str': str,
    'char': str,
    'integer': int,
    'int': int,
    'float': float,
    'number': float,
    'numeric': float
}

CKAN_DATASTORE = config.get(u'ckan.datastore.database')
CKAN_SITE_URL = config.get(u'ckan.site_url')


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
        history_stage = [{'$match': {'$or': [{'_valid_to': {'$exists': False}},
                                             {'_valid_to': {'$gt': timestamp}}],
                                     '_created': {'$lte': timestamp}}},
                         {'$group': group_expression},
                         {'$match': {'_deleted': {'$not': {'$eq': True}}}},
                         {'$project': {'_id': 0, '_deleted': 0, '_created': 0}}]

    return history_stage


def convert_fields(record, data_type_dict):
    for key in record:
        if record[key]:
            if key not in data_type_dict:
                continue
            if data_type_dict[key] == float and (record[key] == '' or str(record[key]).isspace()):
                record[key] = None
            else:
                try:
                    record[key] = data_type_dict[key](record[key])
                except ValueError as e:
                    print(e)
    return record


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

        def __get_collections(self, resource_id):
            col = self.datastore.get_collection(resource_id)
            meta = self.datastore.get_collection('{0}_meta'.format(resource_id))
            fields = self.datastore.get_collection('{0}_fields'.format(resource_id))
            return col, meta, fields

        def __get_max_id(self, resource_id):
            col, _, _ = self.__get_collections(resource_id)

            max_id_results = list(
                col.aggregate([{'$group': {'_id': '', 'max_id': {'$max': '$_id'}}}], allowDiskUse=True))

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
            return [name for name in self.datastore.list_collection_names() if
                    not (name.endswith('_meta') or name.endswith('_fields'))]

        def resource_exists(self, resource_id):
            return resource_id in self.datastore.list_collection_names()

        def create_resource(self, resource_id, primary_key):
            if resource_id not in self.datastore.list_collection_names():
                self.datastore.create_collection(resource_id)
                self.datastore.create_collection('{0}_meta'.format(resource_id))

            self.datastore.get_collection('{0}_meta'.format(resource_id)).insert_one(
                {'record_id': primary_key, 'active': True})

            self.datastore.get_collection(resource_id).create_index([('_created', pymongo.ASCENDING)],
                                                                    name='create_index')

        def delete_resource(self, resource_id, filters={}, force=False):
            col = self.client.get_database(CKAN_DATASTORE).get_collection(resource_id)
            meta = self.client.get_database(CKAN_DATASTORE).get_collection('{0}_meta'.format(resource_id))

            if force:
                if filters == {}:
                    meta.update({}, {'$set': {'active': False}})

                ids_to_delete = col.find(filters, {'_id': 0, 'id': 1})

                for id_to_delete in ids_to_delete:
                    tombstone = {'id': id_to_delete['id'], '_deleted': True}
                    col.insert_one(tombstone)
                    col.update({"_id": tombstone['_id']}, {'$currentDate': {'_created': True}}, upsert=True)
            else:
                meta.insert_one({'_deleted': True})

        def update_schema(self, resource_id, field_definitions):
            collection, _, fields = self.__get_collections(resource_id)
            fields.delete_many({})
            fields.insert_many(field_definitions)

            for field in field_definitions:
                field.pop('_id')

        def upsert(self, resource_id, records, dry_run=False):
            col, meta, fields = self.__get_collections(resource_id)

            record_id_key = meta.find_one()['record_id']
            data_type_dict = {}

            for field in fields.find():
                if field['type'] == 'numeric':
                    data_type_dict[field['id']] = float
                else:
                    data_type_dict[field['id']] = str

            records_without_id = [record for record in records if record_id_key not in record.keys()]

            if len(records_without_id) > 0:
                raise MongoDbControllerException('For a datastore upsert, an id '
                                                 'value has to be set for every record. '
                                                 'In this collection the id attribute is "{0}"'.format(record_id_key))

            for record in records:
                if not dry_run:
                    converted_record = convert_fields(record, data_type_dict)
                    if self.__update_required(resource_id, converted_record, record_id_key):
                        old_record = col.find_one({'id': record['id'], '_valid_to': {'$exists': False}})
                        col.insert_one(converted_record)
                        col.update({"_id": converted_record['_id']},
                                   {'$currentDate': {'_created': True}}, upsert=True)
                        result = col.find_one({'_id': record['_id']})
                        if old_record:
                            col.update({'_id': old_record['_id']}, {'$set': {'_valid_to': result['_created']}})
                        record.pop('_id')

        def execute_stored_query(self, pid, offset, limit, records_format='objects'):
            q = self.querystore.retrieve_query(pid)

            if q:
                col, meta, _ = self.__get_collections(q.resource_id)
                # pipeline = json.JSONDecoder(object_pairs_hook=OrderedDict).decode(q.query)
                pipeline = json.loads(q.query, object_hook=json_util.object_hook)
                pagination_stage = []

                if offset and offset > 0:
                    pagination_stage.append({'$skip': offset})

                if limit:
                    if 0 < limit <= self.rows_max:
                        pagination_stage.append({'$limit': limit})
                    if limit < self.rows_max:
                        pagination_stage.append({'$limit': self.rows_max})
                        limit = self.rows_max

                result = {'records': list(col.aggregate(pipeline + pagination_stage)), 'pid': pid}

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

                query = json.JSONDecoder(object_pairs_hook=OrderedDict).decode(q.query)
                projection = [projection for projection in query if '$project' in projection.keys()]

                if projection:
                    projection = projection[-1]['$project']

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
                                projected_schema, records_format='objects'):

            timestamp = datetime.now(pytz.UTC)

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

            query, meta_data = self.querystore.store_query(resource_id, result['query'], str(timestamp),
                                                           result['records_hash'], HASH_ALGORITHM().name,
                                                           projected_schema)

            result['pid'] = query.id

            result['metadata'] = meta_data

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

            query = json.dumps(history_stage + pipeline, default=json_util.default)

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

            meta_entry = meta.find_one({}, {"_id": 0})
            schema = fields.find({}, {'_id': 0})

            return {'meta': meta_entry, 'schema': list(schema)}

    @classmethod
    def get_instance(cls):
        if VersionedDataStoreController.instance is None:
            log.info(config.get(u'ckan.datastore.write_url'))
            client = MongoClient(config.get(u'ckan.datastore.write_url'))
            querystore = QueryStoreController(config.get(u'ckan.querystore.url'))
            rows_max = config.get(u'ckan.datastore.search.rows_max', 100)
            VersionedDataStoreController.instance = VersionedDataStoreController.__VersionedDataStoreController(client,
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


class QueryStoreController:
    def __init__(self, querystore_url):
        self.engine = create_engine(querystore_url, echo=False)
        cred = PIDClientCredentials.load_from_JSON('/etc/ckan/default/cred.json')
        self.handle_client = PyHandleClient('rest').instantiate_with_credentials(cred)

    def _create_handle_entry(self, pid):
        landing_page = CKAN_SITE_URL + '/querystore/view_query?id=' + str(pid)
        api_url = CKAN_SITE_URL + '/api/3/action/querystore_resolve?pid=' + str(pid)
        handle = self.handle_client.generate_and_register_handle('TEST', landing_page)
        self.handle_client.modify_handle_value(handle, ttl=None, add_if_not_exist=True,
                                               API_URL={'format': 'string', 'value': api_url})

        return handle

    def store_query(self, resource_id, query, timestamp, result_hash,
                    hash_algorithm, projected_schema):
        Session = sessionmaker(bind=self.engine)
        session = Session()

        query_hash = calculate_hash(query)
        if projected_schema:
            record_field_hash = calculate_hash(projected_schema)
        else:
            record_field_hash = None

        q = session.query(Query).filter(Query.query_hash == query_hash,
                                        Query.result_set_hash == result_hash,
                                        Query.record_field_hash == record_field_hash).first()

        resource_metadata = get_action('resource_show')(None, {'id': resource_id})
        package_metadata = get_action('package_show')(None, {'id': resource_metadata['package_id']})

        if q:
            meta_data = {}
            for meta_field in session.query(MetaDataField).filter(MetaDataField.query_id == q.id):
                meta_data[meta_field.key] = meta_field.value

            return q, meta_data
        else:
            q = Query()
            q.resource_id = resource_id
            q.query = query,
            q.query_hash = query_hash
            q.result_set_hash = result_hash
            q.timestamp = timestamp
            q.hash_algorithm = hash_algorithm
            q.record_field_hash = record_field_hash

            metadata = {
                'citation_title': package_metadata['title'],
                'citation_author': package_metadata['author'],
                'citation_maintainer': package_metadata['maintainer'],
                'citation_filename': resource_metadata['name']
            }

            for entry in package_metadata['extras']:
                metadata['citation_' + entry['key']] = entry['value']

            session.add(q)
            session.commit()

            q.handle_pid = self._create_handle_entry(q.id)
            session.merge(q)
            session.commit()
            metadata['citation_handle_pid'] = q.handle_pid

            for key in metadata:
                meta_entry = MetaDataField()
                meta_entry.key = key
                meta_entry.value = metadata[key]
                meta_entry.query_id = q.id
                session.add(meta_entry)

            if projected_schema:
                for field in projected_schema:
                    r = RecordField()
                    r.name = field['id']
                    r.datatype = field['type']

                    if 'info' in field.keys() and 'label' in field['info'].keys():
                        r.description = field['info']['label']

                    if 'info' in field.keys() and 'label' in field['info'].keys():
                        if r.description:
                            r.description += ' - ' + field['info']['notes']
                        else:
                            r.description = field['info']['notes']

                    r.query_id = q.id
                    session.add(r)
            session.commit()

            return q, metadata

    def retrieve_query(self, pid):
        Session = sessionmaker(bind=self.engine)
        session = Session()
        return session.query(Query).filter(Query.id == pid).first()

    def get_cursor_on_ids(self):
        Session = sessionmaker(bind=self.engine)
        session = Session()
        return session.query(Query.id).all()

    def purge_query_store(self):
        Session = sessionmaker(bind=self.engine)
        session = Session()

        session.query(Query).delete()

        session.commit()
