from ckanext.datastore.backend import DatastoreException


class MongoDbControllerException(DatastoreException):
    pass


class IdMismatch(MongoDbControllerException):
    pass


class QueryNotFoundException(MongoDbControllerException):
    pass


class QueryStoreException(DatastoreException):
    pass
