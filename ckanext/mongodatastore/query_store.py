import logging

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from ckanext.mongodatastore.model import Query

log = logging.getLogger(__name__)


class QueryStoreException(Exception):
    pass


class QueryStore:

    def __init__(self, querystore_url):
        self.engine = create_engine(querystore_url, echo=False)

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
