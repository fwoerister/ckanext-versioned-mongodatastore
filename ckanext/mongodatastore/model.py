from sqlalchemy import Column, BIGINT, TEXT, INTEGER
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class Query(Base):
    def __init__(self):
        pass

    def to_dict(self):
        return {'id': self.id,
                'resource_id': self.resource_id,
                'statement': self.statement,
                'projection': self.projection,
                'sort': self.sort,
                'statement_hash': self.statement_hash,
                'projection_hash': self.projection_hash,
                'result_set_hash': self.result_set_hash,
                'sort_hash': self.sort_hash,
                'timestamp': self.timestamp,
                }

    __tablename__ = 'query'

    id = Column(BIGINT, primary_key=True)
    resource_id = Column(TEXT)
    query = Column(TEXT)
    query_with_removed_ts = Column(TEXT)
    query_hash = Column(TEXT)
    hash_algorithm = Column(TEXT)
    result_set_hash = Column(TEXT)
    timestamp = Column(TEXT)
