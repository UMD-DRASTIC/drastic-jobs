"""This module defines asyncronous tasks for CI-BER workflow"""
from __future__ import absolute_import
from workers.celery import app
from celery.utils.log import get_task_logger
import os
import functools
from cli.client import IndigoClient
import requests
from requests_toolbelt import MultipartEncoder
import json
from bs4 import BeautifulSoup
from contextlib import closing
from requests.exceptions import ConnectionError
from index.util import add_BD_fields_legacy, readMaxText
from requests.auth import HTTPBasicAuth

clowder_url = os.getenv('CLOWDER_URL', 'http://localhost:9000')
clowder_auth_encoded = os.getenv('CLOWDER_AUTH_ENCODED')
clowder_commkey = os.getenv('CLOWDER_COMMKEY', 'foo')
clowder_spaceid = os.getenv('CLOWDER_SPACE_ID')
indigo_url = os.getenv('INDIGO_URL', 'http://localhost')
cdmi_proxy_url = os.getenv('CDMI_PROXY_URL', 'http://localhost')
indigo_user = os.getenv('INDIGO_USER', 'worker')
indigo_password = os.getenv('INDIGO_PASSWORD', 'password')
indigo_auth = HTTPBasicAuth(indigo_user, indigo_password)
elasticsearch_url = os.getenv('ELASTICSEARCH_URL', 'http://localhost:9200')
dap_url = os.getenv("DAP_URL", 'http://localhost:8184')
dap_auth_encoded = os.getenv('DAP_AUTH_ENCODED')
fulltext_max_index_size = 10000000  # 10mb is approx. 2500 pages of text

logger = get_task_logger(__name__)


__client = None


class Error(Exception):
    pass

    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


class AuthError(Exception):
    pass


class CDMIError(Error):
    pass


class ClowderNoExtractorsError(Error):
    pass


class BDError(Error):
    pass


class DelayedTraverseError(Error):
    pass


def get_client():
    global __client
    if __client is None:
        myclient = IndigoClient(indigo_url)
        res = myclient.authenticate(indigo_user, indigo_password)
        if not res.ok():
            logger.error("Failed to authenticate: {0}".format(res.msg()))
            raise AuthError
        else:
            __client = myclient
    return __client


def get_cdmi(path):
    client = get_client()
    res = client.get_cdmi(str(path))
    if not res.ok():
        raise CDMIError(res.msg())
    return res.json()


def get_cdmi_content_stream(path):
    url = indigo_url + '/api/cdmi/' + path
    headers = {'Accept-Encoding': 'identity'}
    resp = requests.get(url, auth=indigo_auth, headers=headers, stream=True)
    resp.raise_for_status()
    raw = resp.raw
    raw.read = functools.partial(resp.raw.read, decode_content=True)
    if 'content-length' in resp.headers:
        raw.len = int(resp.headers['content-length'])
    return raw


def get_download_content_stream(path):
    s = requests.Session()
    login = s.get(indigo_url + '/users/login')
    login.raise_for_status()
    bs = BeautifulSoup(login.text, "lxml")
    csrfmiddlewaretoken = None
    for input in bs.select('input'):
        if input['name'] == 'csrfmiddlewaretoken':
            csrfmiddlewaretoken = input['value']
            break
    data = {'username': indigo_user,
            'password': indigo_password,
            'csrfmiddlewaretoken': csrfmiddlewaretoken}
    res = s.post(indigo_url + '/users/login', data=data)
    res.raise_for_status()
    url = indigo_url + '/archive/download/' + path
    headers = {'Accept-Encoding': 'identity'}
    resp = s.get(url, headers=headers, stream=True)
    resp.raise_for_status()
    raw = resp.raw
    raw.read = functools.partial(resp.raw.read, decode_content=True)
    if 'content-length' in resp.headers:
        raw.len = int(resp.headers['content-length'])
    return raw


def get_cdmi_content(path):
    url = indigo_url + '/api/cdmi/' + path
    with closing(requests.get(url, auth=indigo_auth)) as resp:
        resp.raise_for_status()
        return resp.content


def get_download_content(path):
    s = requests.Session()
    login = s.get(indigo_url + '/users/login')
    login.raise_for_status()
    bs = BeautifulSoup(login.text, "lxml")
    csrfmiddlewaretoken = None
    for input in bs.select('input'):
        if input['name'] == 'csrfmiddlewaretoken':
            csrfmiddlewaretoken = input['value']
            break
    data = {'username': indigo_user,
            'password': indigo_password,
            'csrfmiddlewaretoken': csrfmiddlewaretoken}
    res = s.post(indigo_url + '/users/login', data=data)
    res.raise_for_status()
    url = indigo_url + '/archive/download/' + path
    with closing(s.get(url)) as resp:
        resp.raise_for_status()
        return resp.content


def put_metadata(path, metadata):
    get_client().put(path, metadata=metadata)


@app.task
def react(operation, object_type, path, stateChange):
    """Reacts to the state changes indicated by parameters, queuing up other
    tasks"""
    path = path[:-1] if path.endswith('?') else path
    if 'create' == operation:
        index.apply_async((path,))
        if 'resource' == object_type:
            fileWorkflow()
    elif operation in ["update_object", "update_metadata"]:
        index.apply_async((path,))
    elif "delete" == operation:
        deindex.apply_async((path, object_type))


@app.task
def fileWorkflow(path):
    path = path[:-1] if path.endswith('?') else path
    postForExtract.apply_async((path,))
    cdmi_info = get_cdmi(path)
    if 'text/plain' != cdmi_info.get('mimetype'):
        textConversion.apply_async((path,))


@app.task(throws=(AuthError, CDMIError))
def index(path):
    """Reindexes the metadata for a data object"""
    path = path[:-1] if path.endswith('?') else path
    mytype = 'folder' if str(path).endswith('/') else 'file'
    deleteIndexByQuery(path, mytype)

    esdoc = {}
    esdoc['path'] = str(path)
    esdoc['pathtext'] = str(path)
    cdmi_info = get_cdmi(path)

    # Indigo fields:
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
        stream = get_download_content_stream(path)
        esdoc['fulltext'] = readMaxText(stream, fulltext_max_index_size)
    elif 'fulltext' in cdmi_info['metadata']:
        esdoc['fulltext'] = cdmi_info['metadata'].get('fulltext')

    logger.debug('ESDOC:\n{0}'.format(json.dumps(esdoc)))
    url = elasticsearch_url+'/indigo/'+mytype
    r = requests.post(url, data=json.dumps(esdoc))
    if r.status_code != requests.codes.created:
        logger.error('ES status: {0} {1}'.format(r.status_code, r.text))


@app.task
def deindex(path):
    """Removes a data object from the index"""
    path = path[:-1] if path.endswith('?') else path
    mytype = 'folder' if str(path).endswith('/') else 'file'
    logger.info('Deindex task launched for: {0}'.format(path))
    deleteIndexByQuery(path, mytype)


# uses delete-by-query plugin, see docs
def deleteIndexByQuery(path, mytype):
    body = {
        "query": {
            "term": {
                "path": str(path)
            }
        }
    }
    url = elasticsearch_url+'/indigo/'+mytype+'/_query'
    r = requests.delete(url, data=json.dumps(body))
    if r.status_code != requests.codes.ok:
        logger.error('ES DELETE BY QUERY failed: {0} {1}'
                     .format(r.status_code, r.text))


@app.task
def postForExtract(path):
    """Post a file to the feature extraction service (DTS)"""

    with closing(get_download_content_stream(path)) as stream:
        # POST file to DTS and parse fileid
        url = '{0}/api/extractions/upload_file?commkey={1}'.format(
            clowder_url, clowder_commkey)
        m = MultipartEncoder(fields={'File': (os.path.basename(path), stream)})
        headers = {
            'Content-Type': m.content_type,
            'Accept': 'application/json',
            'Authorization': "Basic {0}".format(clowder_auth_encoded)}
        try:
            r = requests.post(url, headers=headers, data=m)
            if r.status_code == requests.codes.ok:
                parsed = r.json()
                fileid = parsed['id']
                pollForExtract.apply_async((path, fileid, 1), delay=10)
                add_to_clowder_space.apply_async((path, fileid))
            else:
                logger.warn('Post for extract failed for {0} with {1} {2}'
                            .format(path, r.status_code, r.text))
        except ConnectionError as e:
            raise CDMIError(str(e))


@app.task
def pollForExtract(path, fileid, retries):
    """Poll the feature extraction service for the results of an extraction.
       Re-enqueue this task if still waiting."""
    url = '{0}/api/extractions/{1}/status?commkey={2}'.format(
        clowder_url, fileid, clowder_commkey)
    r = requests.get(url)
    if r.status_code != requests.codes.ok:
        logger.error('Failed to poll for extract for {0} {1} with {2} {3}'
                     .format(path, fileid, r.status_code, r.text))
        return
    parsed = r.json()
    extractionStatus = parsed['Status']
    doneStatus = ['Done']
    failStatus = ['No Extractor Available. Request is not queued.']
    waitStatus = ['Processing',
                  'Required Extractor is either busy or' +
                  ' is not currently running. Try after some time.']
    if extractionStatus in waitStatus:
        if retries == 0:
            logger.warn('Extract did not complete for {0} {1} with {2}'
                        .format(path, fileid, extractionStatus))
        else:
            pollForExtract.apply_async((path, fileid, retries-1), delay=30)
        return
    elif extractionStatus in failStatus:
        logger.warn('Extract failed for {0} {1} with {2}'
                    .format(path, fileid, extractionStatus))
        raise ClowderNoExtractorsError
    elif extractionStatus not in doneStatus:
        raise BDError('Unrecognized extraction status for {0} {1} with {2}'
                      .format(path, fileid, extractionStatus))
    # GET new metadata
    url = '{0}/api/files/{1}/metadata.jsonld?commkey={2}'.format(
        clowder_url, fileid, clowder_commkey)
    r = requests.get(url)
    r.raise_for_status()
    parsed = r.json()
    logger.debug("fetched metadata: {0}".format(json.dumps(parsed)))

    # Get existing metadata in Indigo
    cdmi_info = get_cdmi(path)
    metadata = cdmi_info['metadata']

    # GET new tags
    try:
        url2 = '{0}/api/files/{1}/tags?commkey={2}'.format(
            clowder_url, fileid, clowder_commkey)
        r2 = requests.get(url2)
        r2.raise_for_status()
        tags = r2.json()['tags']
        metadata['dts_tags.json'] = json.dumps(tags)
        logger.debug("fetched tags: {0}".format(json.dumps(tags)))
    except ConnectionError as e:
        raise BDError(e)

    # Modify existing metadata
    # Create Clowder ID and link field
    metadata['dts_clowder_link'] = '{0}/files/{1}/'.format(clowder_url, fileid)
    metadata['dts_clowder_id'] = fileid
    metadata['dts_metadata.jsonld'] = json.dumps(parsed)

    put_metadata(path, metadata)


@app.task
def textConversion(path):
    """Post a file for conversion to text (DAP)"""
    textLink = None
    try:
        with closing(get_download_content_stream(path)) as stream:
            url = '{0}/convert/txt/'.format(dap_url)
            m = MultipartEncoder(fields={'file': (os.path.basename(path), stream)})
            headers = {
                'Content-Type': m.content_type,
                'Accept': 'text/plain',
                'Authorization': "Basic {0}".format(dap_auth_encoded)}
            r = requests.post(url, headers=headers, data=m)
            if r.status_code == requests.codes.ok:
                textLink = r.text.strip()
                if textLink.endswith('/file/404'):
                    logger.info('No text conversion for {0}'.format(path))
                    return
                logger.info('Got conversion link "{0}" for {1}'
                            .format(textLink, path))
                pollForTextConversion.apply_async(
                    (path, textLink, 1), delay=15)
            else:
                raise BDError('Text conversion failed for {0} with {1} {2}'
                              .format(path, r.status_code, r.text))
    except ConnectionError as e:
        raise BDError(str(e))


@app.task
def pollForTextConversion(path, link, retries):
    """Tries to download text when available."""
    headers = {'Accept': 'text/plain',
               'Authorization': "Basic {0}".format(dap_auth_encoded)}
    r = requests.get(link, headers=headers)
    if r.status_code == 404:
        if retries == 0:
            logger.warn('Text conversion did not complete for {0} {1} with {2}'
                        .format(path, link, "404 Not Found"))
        else:
            pollForTextConversion.apply_async(
                (path, link, retries-1), delay=60)
        return

    cdmi_info = get_cdmi(path)
    metadata = cdmi_info['metadata']
    metadata['fulltext'] = r.text
    put_metadata(path, metadata)


@app.task
def add_to_clowder_space(path, fileid):
    name = os.path.basename(path)
    name = name[:-1] if name.endswith('?') else name
    url = '{0}/api/datasets/createempty'.format(
        clowder_url)
    data = {
        "name": name,
        "description": path,
        "space": clowder_spaceid,
        "existingFiles": ""
        }
    headers = {
        'Accept': 'application/json',
        'Authorization': "Basic {0}".format(clowder_auth_encoded)}
    r = requests.post(url, headers=headers, json=data)
    r.raise_for_status()
    datasetid = r.json()['id']
    url2 = '{0}/api/datasets/{1}/files/{2}' \
        .format(clowder_url, datasetid, fileid)
    r2 = requests.post(url2, headers=headers, json={})
    r2.raise_for_status()
    url3 = '{0}/api/spaces/{1}/addDatasetToSpace/{2}' \
        .format(clowder_url, clowder_spaceid, datasetid)
    r3 = requests.post(url3, headers=headers, json={})
    r3.raise_for_status()


@app.task(throws=(AuthError),
          bind=True,
          max_retries=10,
          rate_limit='30/m')
def traversal(self, path, task_name, only_files):
    """Traverses the file tree under the path given, within the CDMI service.
       Applies the named task to every path."""

    # reschedule this traverse if default queue is already large
    task_count = app.get_message_count('default')
    if(task_count > 50):
        exc = DelayedTraverseError("Delaying Traverse due to queue size: {0}"
                                   .format(task_count))
        raise self.retry(exc=exc)

    path = path[:-1] if path.endswith('?') else path

    client = get_client()
    res = client.ls(path)
    if not res.ok():
        logger.error("CDMI 'ls' request failed: {0} at {1}"
                     .format(res.msg(), path))
        return

    cdmi_info = res.json()
    if not cdmi_info[u'objectType'] == u'application/cdmi-container':
        logger.error("Cannot traverse a file path: {0}".format(path))
        return

    if only_files:
        for f in cdmi_info[u'children']:
            f = f[:-1] if f.endswith('?') else f
            if not f.endswith('/'):
                app.send_task('workers.tasks.'+task_name,
                              args=[str(path)+f], kwargs={})
    else:
        for o in cdmi_info[u'children']:
            o = o[:-1] if o.endswith('?') else o
            app.send_task('workers.tasks.'+task_name,
                          args=[str(path)+o], kwargs={})

    for x in cdmi_info[u'children']:
        x = x[:-1] if x.endswith('?') else x
        if x.endswith('/'):
            traversal.apply_async((str(path)+x, task_name, only_files))


@app.task(throws=(AuthError),
          bind=True,
          max_retries=10,
          rate_limit='30/m')
def traverse_httpdir(self, path, task_name, only_files):
    """Traverses the file tree under the path given, within the CDMI service.
       Applies the named task to every path."""

    # reschedule this traverse if default queue is already large
    task_count = app.get_message_count('default')
    if(task_count > 50):
        exc = DelayedTraverseError("Delaying Traverse due to queue size: {0}"
                                   .format(task_count))
        raise self.retry(exc=exc)

    # Get directory
    res = requests.get(path)
    if not res.ok():
        logger.error("FTP-over-HTTP GET request failed: {0} at {1}"
                     .format(res.msg(), path))
        return

    dir_info = res.json()

    if only_files:
        for f in dir_info:
            if 'file' == f['type']:
                app.send_task('workers.tasks.'+task_name,
                              args=[str(path)+f], kwargs={})
    else:
        for o in dir_info:
            app.send_task('workers.tasks.'+task_name,
                          args=[str(path)+o], kwargs={})

    for x in dir_info:
        if 'directory' == x['type']:
            traverse_httpdir.apply_async((str(path)+x+'/', task_name, only_files))
