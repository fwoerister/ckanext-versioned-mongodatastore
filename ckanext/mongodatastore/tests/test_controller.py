import unittest
import StringIO

from mock import patch, MagicMock

from ckanext.mongodatastore.controller import history_dump_to

mock_xml = MagicMock()
mock_xml.side_effect = [{'limit': 1, 'fields': [{'id': 'name', 'type': 'string'}], 'records': [{'name': 'florian'}]},
                        {'limit': 1, 'fields': [{'id': 'name', 'type': 'string'}], 'records': []}]

mock_json = MagicMock()
mock_json.side_effect = [{'limit': 1, 'fields': [{'id': 'name', 'type': 'string'}], 'records': [{'name': 'florian'}]},
                         {'limit': 1, 'fields': [{'id': 'name', 'type': 'string'}], 'records': []}]

mock_csv = MagicMock()
mock_csv.side_effect = [
    {'limit': 1, 'fields': [{'id': 'id', 'type': 'number'}, {'id': 'name', 'type': 'string'}], 'records': '1,florian'},
    {'limit': 1, 'fields': [{'id': 'id', 'type': 'number'}, {'id': 'name', 'type': 'string'}], 'records': None}]


class ControllerTest(unittest.TestCase):

    @patch('ckanext.mongodatastore.controller.get_action', return_value=mock_xml)
    def test_history_dump_to_xml(self, mock):
        test_stream = StringIO.StringIO()

        history_dump_to(1, test_stream, 'xml', 0, 1, {})

        print(test_stream.getvalue())

        self.assertEqual(test_stream.getvalue(), '<data>\n<row><name>florian</name></row>\n</data>\n')

    @patch('ckanext.mongodatastore.controller.get_action', return_value=mock_json)
    def test_history_dump_to_json(self, mock):
        test_stream = StringIO.StringIO()

        history_dump_to(1, test_stream, 'json', 0, 1, {})

        print(test_stream.getvalue())

        self.assertEqual(test_stream.getvalue(),
                         u'{\n  "fields": [{"type":"string","id":"name"}],\n  '
                         u'"records": [\n    {"name":"florian"}\n]}\n')

    @patch('ckanext.mongodatastore.controller.get_action', return_value=mock_csv)
    def test_history_dump_to_csv(self, mock):
        test_stream = StringIO.StringIO()

        history_dump_to(1, test_stream, 'csv', 0, 1, {})

        print(test_stream.getvalue())

        self.assertEqual(test_stream.getvalue(), 'id,name\r\n1,florian')

    def test_show_import(self):
        pass

    def test_import_table(self):
        pass
