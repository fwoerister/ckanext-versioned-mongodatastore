import unittest

from ckan.common import config
from sqlalchemy.orm import sessionmaker

from ckanext.mongodatastore.model import Query
from ckanext.mongodatastore.controller.storage_controller import VersionedDataStoreController, QueryStoreController

TEST_RESOURCE_NAME = 'test_resource'
PRIMARY_KEY = 'id'

CKAN_DATASTORE = config.get(u'ckan.datastore.database')
QUERY_STORE_URL = config.get(u'ckan.querystore.url')


class QueryStoreTest(unittest.TestCase):

    def setUp(self):
        cntr = VersionedDataStoreController.get_instance()
        cntr.create_resource(TEST_RESOURCE_NAME, PRIMARY_KEY)
        self.querystore = QueryStoreController(QUERY_STORE_URL)
        self.querystore.purge_query_store()

    def tearDown(self):
        instance = VersionedDataStoreController.get_instance()
        instance.client.drop_database(CKAN_DATASTORE)
        instance.datastore = instance.client.get_database(CKAN_DATASTORE)

    def test_store_query(self):
        pid = self.querystore.store_query(TEST_RESOURCE_NAME, '{}', '5c86a1aaf6b1b8295695c666',
                                          'result_hash', 'query_hash', 'hash_algorithm')

        session = sessionmaker(bind=self.querystore.engine)
        session = session()

        q = session.query(Query).filter(Query.id == pid).first()

        self.assertEqual(q.id, pid)
        self.assertEqual(q.resource_id, TEST_RESOURCE_NAME)
        self.assertEqual(q.query, '{}')
        self.assertEqual(q.query_hash, 'query_hash')
        self.assertEqual(q.result_set_hash, 'result_hash')
        self.assertEqual(q.timestamp, '5c86a1aaf6b1b8295695c666')
        self.assertEqual(q.hash_algorithm, 'hash_algorithm')

        new_pid = self.querystore.store_query(TEST_RESOURCE_NAME, '{}', '5c86a1aaf6b1b8295695c666',
                                              'different_result_hash', 'query_hash', 'hash_algorithm')

        self.assertNotEqual(new_pid, pid)

        new_pid = self.querystore.store_query(TEST_RESOURCE_NAME, '{}', '5c86a1aaf6b1b8295695c666',
                                              'result_hash', 'different_query_hash', 'hash_algorithm')

        self.assertNotEqual(new_pid, pid)

        new_pid = self.querystore.store_query(TEST_RESOURCE_NAME, '{}', '5c86a1aaf6b1b8295695c666',
                                              'result_hash', 'query_hash', 'hash_algorithm')

        self.assertEqual(new_pid, pid)

    def test_retrieve_query(self):
        pid = self.querystore.store_query(TEST_RESOURCE_NAME, '{}', '5c86a1aaf6b1b8295695c666',
                                          'result_hash', 'query_hash', 'hash_algorithm')

        q = self.querystore.retrieve_query(pid)

        self.assertEqual(q.id, pid)
        self.assertEqual(q.resource_id, TEST_RESOURCE_NAME)
        self.assertEqual(q.query, '{}')
        self.assertEqual(q.query_hash, 'query_hash')
        self.assertEqual(q.result_set_hash, 'result_hash')
        self.assertEqual(q.timestamp, '5c86a1aaf6b1b8295695c666')
        self.assertEqual(q.hash_algorithm, 'hash_algorithm')

    def test_cursor_on_ids(self):
        pid = self.querystore.store_query(TEST_RESOURCE_NAME, '{}', '5c86a1aaf6b1b8295695c666',
                                          'result_hash', 'query_hash', 'hash_algorithm')

        ids = self.querystore.get_cursor_on_ids()

        for c in ids:
            self.assertEqual(c[0], pid)

    def test_purge_query_store(self):
        pid = self.querystore.store_query(TEST_RESOURCE_NAME, '{}', '5c86a1aaf6b1b8295695c666',
                                          'result_hash', 'query_hash', 'hash_algorithm')

        q = self.querystore.retrieve_query(pid)

        self.assertEqual(q.id, pid)
        self.assertEqual(q.resource_id, TEST_RESOURCE_NAME)
        self.assertEqual(q.query, '{}')
        self.assertEqual(q.query_hash, 'query_hash')
        self.assertEqual(q.result_set_hash, 'result_hash')
        self.assertEqual(q.timestamp, '5c86a1aaf6b1b8295695c666')
        self.assertEqual(q.hash_algorithm, 'hash_algorithm')

        self.querystore.purge_query_store()

        self.assertIsNone(self.querystore.retrieve_query(pid))
