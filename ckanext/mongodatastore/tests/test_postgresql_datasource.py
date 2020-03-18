import unittest

from mock import MagicMock, patch
from sqlalchemy import create_engine

from ckanext.mongodatastore.datasource.postgresql import PostgreSqlDatasource

DATASOURCE_URL = 'postgresql://test_import_db:test_import_db@localhost/test_import_db'


class PostgreSqlDatasourceTest(unittest.TestCase):
    def setUp(self):
        self.datasource = PostgreSqlDatasource(DATASOURCE_URL)

        self.engine = create_engine(DATASOURCE_URL, echo=False)
        self.engine.execute('drop table if exists dummy_data;')
        self.engine.execute('create table dummy_data (id integer primary key, name text);')

    def __insert_testdata(self):
        self.engine.execute("insert into dummy_data (id, name) values (1,'Florian') ")
        self.engine.execute("insert into dummy_data (id, name) values (2,'Stefan') ")

    def test_is_reachable(self):
        unreachable_datasource = PostgreSqlDatasource('postgresql://non_existing:non_existing@localhost/non_existing')
        self.assertFalse(unreachable_datasource.is_reachable())
        self.assertTrue(self.datasource.is_reachable())

    def test_get_primary_key_name(self):
        primary_key = self.datasource.get_primary_key_name('dummy_data')
        self.assertEqual(primary_key, 'id')

    def test_get_available_datasets(self):
        available_datasets = self.datasource.get_available_datasets()
        self.assertEqual(available_datasets, ['dummy_data'])

    @patch('ckanext.mongodatastore.datasource.postgresql.get_action')
    @patch('ckanext.mongodatastore.datasource.postgresql.c')
    def test_migrate_records_to_datasource(self, c_mock, get_action_mock):
        self.__insert_testdata()

        upsert_mock = MagicMock()

        def action(name):
            if name == 'datastore_upsert':
                return upsert_mock

        get_action_mock.side_effect = action

        self.datasource.migrate_records_to_datasource('dummy_data', 123, 'upsert')

        upsert_mock.assert_called_with(None, {'resource_id': 123,
                                              'force': True,
                                              'records': [{'id': 1, 'name': 'Florian'}, {'id': 2, 'name': 'Stefan'}],
                                              'method': 'upsert',
                                              'calculate_record_count': False})

    @patch('ckanext.mongodatastore.datasource.postgresql.get_action')
    @patch('ckanext.mongodatastore.datasource.postgresql.c')
    def test_migrate_records_to_datasource_with_empty_table(self, c_mock, get_action_mock):
        upsert_mock = MagicMock()

        def action(name):
            if name == 'datastore_upsert':
                return upsert_mock

        get_action_mock.side_effect = action

        self.datasource.migrate_records_to_datasource('dummy_data', 123, 'upsert')

        upsert_mock.assert_not_called()

    def test_get_protocol(self):
        self.assertEqual(self.datasource.get_protocol(), 'postgresql')
