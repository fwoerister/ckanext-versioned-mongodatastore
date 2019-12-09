import logging

from ckan import logic

from ckanext.mongodatastore.mongodb_controller import MongoDbController

log = logging.getLogger(__name__)


@logic.side_effect_free
def querystore_resolve(context, data_dict):
    cntr = MongoDbController.getInstance()

    pid = data_dict.get('pid')
    skip = data_dict.get('offset', None)
    limit = data_dict.get('limit', None)

    if skip:
        skip = int(skip)
    if limit:
        limit = int(limit)

    records_format = data_dict.get('records_format', 'objects')

    log.debug('querystore_resolve parameters {0}'.format([pid, skip, limit, records_format]))

    result = cntr.retrieve_stored_query(pid, offset=skip, limit=limit, records_format=records_format)

    log.debug('querystore_resolve result: {0}'.format(result))

    return result


def datastore_restore(context, data_dict):
    raise NotImplementedError()
