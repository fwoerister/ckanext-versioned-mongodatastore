from locust import HttpLocust, TaskSet, task

import random

LOADTEST_USER = 'fwoerister'
LOADTEST_API_KEY = 'aab7917c-aa29-4bec-80b5-8a110cf78839'
LOADTEST_RESOURCE_ID = 'a58777a0-2480-45f5-a26e-3314f7f1df4c'
LOADTEST_OWNER_ORG = 'tu-wien'

SCHEMA_FIELDS = ["id", "class",
                 "mean-radius",
                 "mean-texture",
                 "mean-perimeter",
                 "mean-area",
                 "mean-smoothness",
                 "mean-compactness",
                 "mean-concavity",
                 "mean-concave-points",
                 "mean-symmetry",
                 "mean-fractal-dimension",
                 "se-radius",
                 "se-texture",
                 "se-perimeter",
                 "se-area",
                 "se-smoothness",
                 "se-compactness",
                 "se-concavity",
                 "se-concave-points",
                 "se-symmetry",
                 "se-fractal-dimension",
                 "worst-radius",
                 "worst-texture",
                 "worst-perimeter",
                 "worst-area",
                 "worst-smoothness",
                 "worst-compactness",
                 "worst-concavity",
                 "worst-concave-points",
                 "worst-symmetry",
                 "worst-fractal-dimension"]


def record_to_str(record):
    record_str = '{"id": ' + str(record['id']) + ', "class": "' + str(record['class']) + '"'

    for key in record.keys():
        if key != 'id' and key != 'class':
            record_str += ', "{0}": {1}'.format(key, str(record[key]))

    record_str += '}'
    return record_str


class ContentPublisher(TaskSet):
    @task(2)
    def add_random_random_record(self):
        headers = {'Authorization': LOADTEST_API_KEY,
                   'content-type': 'application/json'}

        record = {
            "id": random.randint(1, 99999999),
            "class": random.choice(['M', 'B'])}

        for field in SCHEMA_FIELDS:
            if field != 'id' and field != 'class':
                record[field] = random.random()

        payload = '{"resource_id": "' + LOADTEST_RESOURCE_ID + '", "force":true, ' \
                  + '"records": [' + record_to_str(record) + '], ' + '"method": "upsert"}'

        self.client.post("/api/3/action/datastore_upsert", data=payload, headers=headers)

    @task(2)
    def submit_query(self):
        filter_field = random.choice([field for field in SCHEMA_FIELDS if field != 'id' and field != 'class'])

        payload = '{"resource_id": "' + LOADTEST_RESOURCE_ID + '", ' \
                  + '"filters": {"' + filter_field + '":' + str(random.random()) + '}}'

        headers = {'Authorization': LOADTEST_API_KEY,
                   'content-type': 'application/json'}

        self.client.post("/api/3/action/datastore_search", data=payload, headers=headers)


class WebsiteUser(HttpLocust):
    task_set = ContentPublisher
    min_wait = 1000
    max_wait = 10000
