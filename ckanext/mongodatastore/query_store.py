import logging
from ckan.common import config

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from ckanext.mongodatastore.model import Query

from pyhandle.clientcredentials import PIDClientCredentials
from pyhandle.handleclient import PyHandleClient

log = logging.getLogger(__name__)

CKAN_SITE_URL = config.get(u'ckan.site_url')


class QueryStoreException(Exception):
    pass


class QueryStore:

    def __init__(self, querystore_url):
        self.engine = create_engine(querystore_url, echo=False)
        cred = PIDClientCredentials.load_from_JSON('/etc/ckan/default/cred.json')
        self.handle_client = PyHandleClient('rest').instantiate_with_credentials(cred)

    def _create_handle(self, pid):
        landing_page = CKAN_SITE_URL + '/querystore/view_query?id=' + str(pid)
        api_url = CKAN_SITE_URL + '/api/3/action/querystore_resolve?pid=' + str(pid)
        handle = self.handle_client.generate_and_register_handle('99.9999', landing_page)
        self.handle_client.modify_handle_value(handle, ttl=None, add_if_not_exist=True,
                                               API_URL={'format': 'string', 'value': api_url})

        return handle

    def store_query(self, resource_id, query, timestamp, result_hash, query_hash,
                    hash_algorithm):
        Session = sessionmaker(bind=self.engine)
        session = Session()

        q = session.query(Query).filter(Query.query_hash == query_hash,
                                        Query.result_set_hash == result_hash).first()

        if q:
            return q.id
        else:
            q = Query()
            q.resource_id = resource_id
            q.query = query,
            q.query_hash = query_hash
            q.result_set_hash = result_hash
            q.timestamp = timestamp
            q.hash_algorithm = hash_algorithm

            session.add(q)
            session.commit()

            q.handle_pid = self._create_handle(q.id)

            session.merge(q)
            session.commit()

            return q.id

    def retrieve_query(self, pid):
        Session = sessionmaker(bind=self.engine)
        session = Session()
        return session.query(Query).filter(Query.id == pid).first()

    def get_cursoer_on_ids(self):
        Session = sessionmaker(bind=self.engine)
        session = Session()
        return session.query(Query.id).all()

    def purge_query_store(self):
        Session = sessionmaker(bind=self.engine)
        session = Session()

        session.query(Query).delete()

        session.commit()
