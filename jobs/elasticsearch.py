"""Elasticsearch Workflow"""
import os
from jobs.celery_app import app
from celery.utils.log import get_task_logger
import requests
from jobs.util import get_client, stream_from_drastic_proxy
from jobs import workflow

__copyright__ = "Copyright (C) 2016 University of Maryland"
__license__ = "GNU AFFERO GENERAL PUBLIC LICENSE, Version 3"

logger = get_task_logger(__name__)
elasticsearch_url = os.getenv('ELASTICSEARCH_URL', 'http://localhost:9200')
fulltext_max_index_size = 10000000  # 10mb is approx. 2500 pages of text


def react(event):
    if 'create' == event.operation:
        index.apply_async((event.path,))
    elif event.operation in ["update_object", "update", "update_metadata"]:
        index.apply_async((event.path,))
    elif "delete" == event.operation:
        deindex.apply_async((event.path, event.object_type))


workflow.registry.subscribe(react)


@app.task(bind=True, default_retry_delay=300, max_retries=10)
def index(self, path):
    """Reindexes the metadata for a data object"""
    from index.util import add_BD_fields_legacy, readMaxText
    path = path[:-1] if path.endswith('?') else path
    mytype = 'folder' if str(path).endswith('/') else 'file'

    esdoc = {}
    esdoc['path'] = str(path)
    esdoc['pathtext'] = str(path)
    try:
        res = get_client().get_cdmi(str(path))
        if res.code() in [404, 403]:
            logger.warn("Dropping task for object that gives a 403/403: {0}".format(path))
            return
        if not res.ok():
            raise IOError("Drastic get_cdmi failed: {0}".format(res.msg()))
        cdmi_info = res.json()
    except IOError as e:
        raise self.retry(exc=e)

    # Drastic fields:
    # FIXME name is not the key, is null
    name = cdmi_info.get('objectName')
    esdoc['objectName'] = name[:-1] if name.endswith('?') else name
    esdoc['objectID'] = cdmi_info.get('objectID')
    esdoc['parentID'] = cdmi_info.get('parentID')
    esdoc['parentURI'] = cdmi_info.get('parentURI')

    esdoc['mimetype'] = cdmi_info.get('mimetype')
    # TODO esdoc['size'] = cdmi_info.get('size')

    # If we have extracted metadata from Brown Dog, add any mapped fields
    if 'dts_metadata.jsonld' in cdmi_info.get('metadata'):
        add_BD_fields_legacy(cdmi_info['metadata']
                             .get('dts_metadata.jsonld', '[]'), esdoc)

    if 'dts_tags.json' in cdmi_info.get('metadata'):
        esdoc['dts_tags'] = cdmi_info['metadata'].get('dts_tags.json')

    # if file mimetype is already text/plain, index it as fulltext
    if 'text/plain' == cdmi_info.get('mimetype'):
        try:
            with closing(stream_from_drastic_proxy(path)) as stream:
                esdoc['fulltext'] = readMaxText(stream, fulltext_max_index_size)
        except IOError as e:
            logger.warn("Cannot get original object text for indexing: {0}".format(str(e)))
    elif 'fulltext' in cdmi_info['metadata']:
        esdoc['fulltext'] = cdmi_info['metadata'].get('fulltext')

    logger.debug('ESDOC:\n{0}'.format(json.dumps(esdoc)))
    url = elasticsearch_url+'/drastic/'+mytype
    try:
        r = requests.post(url, data=json.dumps(esdoc))
        if r.status_code != requests.codes.created:
            logger.error('ES status: {0} {1}'.format(r.status_code, r.text))
    except IOError as e:
        self.retry(exc=e)


@app.task(bind=True, default_retry_delay=300, max_retries=10)
def deindex(self, path):
    """Removes a data object from the index"""
    path = path[:-1] if path.endswith('?') else path
    mytype = 'folder' if str(path).endswith('/') else 'file'
    logger.info('Deindex task launched for: {0}'.format(path))
    body = {
        "query": {
            "term": {
                "path": str(path)
            }
        }
    }
    try:
        url = elasticsearch_url+'/drastic/'+mytype+'/_query'
        r = requests.delete(url, data=json.dumps(body))
        r.raise_for_status()
    except IOError as e:
        raise self.retry(exc=e)
