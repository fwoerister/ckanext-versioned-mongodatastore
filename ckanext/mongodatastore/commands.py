import logging
from datetime import datetime

import click as click
from ckan.common import config as ckan_config
from ckan.lib.cli import paster_click_group, click_config_option, load_config
from sqlalchemy import create_engine

from ckanext.mongodatastore.model import Base
from ckanext.mongodatastore.controller.storage_controller import VersionedDataStoreController

log = logging.getLogger(__name__)

querystore_group = paster_click_group(
    summary=u'Perform commands to set up the querystore')


@querystore_group.command(
    u'create_schema',
    help=u'create query table')
@click.help_option(u'-h', u'--help')
@click_config_option
@click.pass_context
def create_schema(ctx, config):
    load_config(config or ctx.obj['config'])

    log.debug('start creating schema....')

    querystore_url = ckan_config[u'ckan.querystore.url']
    engine = create_engine(querystore_url, echo=True)

    Base.metadata.create_all(engine)
    log.debug('schema created!')


@querystore_group.command(
    u'check_integrity',
    help=u'check if every query result matches the according hash value')
@click.help_option(u'-h', u'--help')
@click_config_option
@click.pass_context
def check_integrity(ctx, config):
    load_config(config or ctx.obj['config'])

    cntr = VersionedDataStoreController.get_instance()
    error_list = []

    start = datetime.utcnow()
    for pid in cntr.querystore.get_cursor_on_ids():
        if not cntr.execute_stored_query(pid, 0, 0, True):
            error_list.append(pid)
            print('query {0} is not valid!'.format(pid))
        else:
            print('query {0} is valid!'.format(pid))

    stop = datetime.utcnow()

    print('integrity check stopped after {0} seconds'.format((stop - start).total_seconds()))
    print('{0} problems detected'.format(len(error_list)))
    if len(error_list) > 0:
        print('The following PIDs do not retrieve a valid result set:')
        for pid in error_list:
            print(pid)
