from sqlalchemy import Column, BIGINT, TEXT, INTEGER
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class Query(Base):
    def __init__(self):
        pass

    __tablename__ = 'query'

    id = Column(BIGINT, primary_key=True)
    resource_id = Column(TEXT)
    query = Column(TEXT)
    query_hash = Column(TEXT)
    hash_algorithm = Column(TEXT)
    result_set_hash = Column(TEXT)
    timestamp = Column(TEXT)
