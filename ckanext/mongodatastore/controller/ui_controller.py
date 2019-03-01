import logging

from ckan.plugins.toolkit import (
    BaseController,
    render,
    get_action,
    h,
    response,
    abort)
from ckanext.datastore.writer import csv_writer, json_writer, xml_writer

from ckanext.mongodatastore.exceptions import QueryNotFoundException

log = logging.getLogger(__name__)

PAGINATE_BY = 100


def history_dump_to(pid, output, fmt, offset, limit, options):
    if not offset:
        offset = 0

    if fmt == 'csv':
        writer_factory = csv_writer
        records_format = 'csv'
    elif fmt == 'json':
        writer_factory = json_writer
        records_format = 'objects'
    elif fmt == 'xml':
        writer_factory = xml_writer
        records_format = 'objects'
    else:
        abort(501, 'Only dump to csv, json or xml file supported!')

    def start_writer(fields):
        bom = options.get(u'bom', False)
        return writer_factory(output, fields, "{0}_dump".format(pid), bom)

    def result_page(offs, lim):

        return get_action('querystore_resolve')(None, dict({'pid': pid,
                                                            'limit':
                                                                PAGINATE_BY if limit is None
                                                                else min(PAGINATE_BY, lim),
                                                            'offset': offs,
                                                            'records_format': fmt,
                                                            'include_total': False}))

    log.debug('call result_page with offset={0} and limit={1}'.format(offset, limit))
    result = result_page(offset, limit)

    log.debug(result)

    if result['limit'] != limit:
        # `limit` (from PAGINATE_BY) must have been more than
        # ckan.datastore.search.rows_max, so datastore_search responded with a
        # limit matching ckan.datastore.search.rows_max. So we need to paginate
        # by that amount instead, otherwise we'll have gaps in the records.
        paginate_by = result['limit']
    else:
        paginate_by = PAGINATE_BY

    log.debug('start writing dump...')

    with start_writer(result['fields']) as wr:
        while True:
            if limit is not None and limit <= 0:
                log.debug('limit is not None and limit <= 0')
                break

            records = result['records']

            log.debug("writing: {0}".format(records))
            wr.write_records(records)
            log.debug("writing done: {0}".format(records))

            if records_format == 'objects' or records_format == 'lists':
                if len(records) < paginate_by:
                    break
            elif not records:
                break

            offset += paginate_by
            if limit is not None:
                limit -= paginate_by
                if limit <= 0:
                    break

            result = result_page(offset, limit)


class QueryStoreUIController(BaseController):
    def view_history_query(self):
        id = h.get_param_int('id')

        try:
            result = get_action('querystore_resolve')(None, {'pid': id,
                                                             'skip': 0,
                                                             'limit': 0})
        except QueryNotFoundException as ex:
            abort(404, 'Unfortunately there is no entry with pid {0} in the query store!'.format(id))

        return render('mongodatastore/query_view.html', extra_vars={'query': result['query'],
                                                                    'result_set': result['records'],
                                                                    'count': len(result['records']),
                                                                    'projection': result['fields']})

    def dump_history_result_set(self):
        pid = int(h.get_request_param('id'))
        format = h.get_request_param('format')
        offset = h.get_request_param('offset')
        limit = h.get_request_param('limit')

        if h.get_request_param('bom') and h.get_request_param('bom') in ['True', 'true']:
            bom = True
        else:
            bom = False

        if offset:
            offset = int(offset)
        if limit:
            limit = int(limit)

        parameters = [
            pid,
            response,
            format,
            offset,
            limit,
            {u'bom': bom}]

        log.debug('history_dump_to parameters: {0}'.format(parameters))

        history_dump_to(
            pid,
            response,
            fmt=format,
            offset=offset,
            limit=limit,
            options={u'bom': bom}
        )
