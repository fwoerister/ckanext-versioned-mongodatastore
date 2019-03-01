import unittest

from ckanext.mongodatastore.helper import normalize_json, calculate_hash

TEST_DATA = {
    'id': 12,
    'name': 'Florian',
    'address': {
        'street': 'TestStreet 12',
        'city': 'TestCity',
        'zip_code': '1234'
    }
}

EXPECTED_KEYS = ['address', 'id', 'name']
EXPECTED_ADDRESS_KEYS = ['city', 'street', 'zip_code']


class HelpersTest(unittest.TestCase):
    def test_normalize_json(self):
        result = normalize_json(TEST_DATA)

        self.assertEqual(list(result.keys()), EXPECTED_KEYS)
        self.assertEqual(list(result['address']), EXPECTED_ADDRESS_KEYS)

    def test_normalize_json_only_first_level(self):
        result = normalize_json(TEST_DATA, max_depth=1)
        self.assertEqual(list(result.keys()), EXPECTED_KEYS)

    def test_calculate_hash_value(self):
        hash_value = calculate_hash(TEST_DATA)
        self.assertEqual(hash_value, '8e9abc4ad418be2f2f1f2d20da38a948')

    def test_calculate_hash_value_of_empty_json(self):
        hash_value = calculate_hash({})
        self.assertEqual(hash_value, 'd41d8cd98f00b204e9800998ecf8427e')
