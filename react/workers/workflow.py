"""This module defines asyncronous tasks for CI-BER workflow"""
from __future__ import absolute_import
from workers.celery_app import app
from workers.util import get_client, stream_from_drastic_proxy
from workers.browndog import postForExtract, textConversion
from celery.utils.log import get_task_logger
import os
import requests
import json
from contextlib import closing
from index.util import add_BD_fields_legacy, readMaxText


elasticsearch_url = os.getenv('ELASTICSEARCH_URL', 'http://localhost:9200')
fulltext_max_index_size = 10000000  # 10mb is approx. 2500 pages of text
AUTOMATIC_FILE_WORKFLOW = False
logger = get_task_logger(__name__)


@app.task
def react(operation, object_type, path, stateChange):
    """Reacts to the state changes indicated by parameters, queuing up other
    tasks"""
    path = path[:-1] if path.endswith('?') else path
    if 'create' == operation:
        index.apply_async((path,))
        if 'resource' == object_type and AUTOMATIC_FILE_WORKFLOW:
            fileWorkflow.apply_async((path,))
    elif operation in ["update_object", "update", "update_metadata"]:
        index.apply_async((path,))
    elif "delete" == operation:
        deindex.apply_async((path, object_type))


@app.task(bind=True, default_retry_delay=300, max_retries=10)
def fileWorkflow(self, path):
    path = path[:-1] if path.endswith('?') else path
    try:
        res = get_client().get_cdmi(str(path))
        if res.code() in [404, 403]:
            logger.warn("Dropping task for object that gives a 403/403: {0}".format(path))
            return
        if not res.ok():
            logger.warn("Error for object that gives {0}: {1}".format(str(res.code()), path))
            raise IOError("Drastic get_cdmi failed: {0} {1}".format(str(res.code()), res.msg()))
        cdmi_info = res.json()
    except IOError as e:
        raise self.retry(exc=e)
    postForExtract.apply_async((path,))
    if 'text/plain' != cdmi_info.get('mimetype'):
        textConversion.apply_async((path,))


@app.task(bind=True, default_retry_delay=300, max_retries=10)
def index(self, path):
    """Reindexes the metadata for a data object"""
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


@app.task(bind=True,
          default_retry_delay=300,
          max_retries=100,
          rate_limit='30/m')
def traversal(self, path, task_name, only_files):
    """Traverses the file tree under the path given, within the CDMI service.
       Applies the named task to every path."""

    app.check_traversal_okay(self)

    path = path[:-1] if path.endswith('?') else path

    try:
        res = get_client().ls(path)
        if res.code() in [404, 403]:  # object probably deleted
            logger.warn("Dropping task for an object that gives a 403/403: {0}".format(path))
            return
        if not res.ok():
            raise IOError(str(res))
    except IOError as e:
        raise self.retry(exc=e)

    cdmi_info = res.json()
    logger.debug('got CDMI content: {0}'.format(json.dumps(cdmi_info)))
    if not cdmi_info[u'objectType'] == u'application/cdmi-container':
        logger.error("Cannot traverse a file path: {0}".format(path))
        return

    if only_files:
        for f in cdmi_info[u'children']:
            f = f[:-1] if f.endswith('?') else f
            if not f.endswith('/'):
                app.send_task(task_name,
                              args=[str(path)+f], kwargs={})
    else:
        for o in cdmi_info[u'children']:
            o = o[:-1] if o.endswith('?') else o
            app.send_task(task_name,
                          args=[str(path)+o], kwargs={})

    for x in cdmi_info[u'children']:
        x = x[:-1] if x.endswith('?') else x
        if x.endswith('/'):
            traversal.apply_async((str(path)+x, task_name, only_files), queue="traversal")
