"""This module defines asyncronous tasks for CI-BER workflow"""
from __future__ import absolute_import
from workers.celery import app
from celery.utils.log import get_task_logger
import os
import functools
from cli.client import DrasticClient
import requests
from requests_toolbelt import MultipartEncoder
import json
from bs4 import BeautifulSoup
from contextlib import closing
from index.util import add_BD_fields_legacy, readMaxText
from requests.auth import HTTPBasicAuth
from urlparse import urlparse
from os.path import basename
import validators


class DrasticNotFoundError(Exception):
    pass

    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


clowder_url = os.getenv('CLOWDER_URL', 'http://localhost:9000')
clowder_auth_encoded = os.getenv('CLOWDER_AUTH_ENCODED')
clowder_commkey = os.getenv('CLOWDER_COMMKEY', 'foo')
clowder_spaceid = os.getenv('CLOWDER_SPACE_ID')
drastic_url = os.getenv('DRASTIC_URL', 'http://localhost')
cdmi_proxy_url = os.getenv('CDMI_PROXY_URL', 'http://localhost')
drastic_user = os.getenv('DRASTIC_USER', 'worker')
drastic_password = os.getenv('DRASTIC_PASSWORD', 'password')
drastic_auth = HTTPBasicAuth(drastic_user, drastic_password)
elasticsearch_url = os.getenv('ELASTICSEARCH_URL', 'http://localhost:9200')
dap_url = os.getenv("DAP_URL", 'http://localhost:8184')
dap_auth_encoded = os.getenv('DAP_AUTH_ENCODED')
fulltext_max_index_size = 10000000  # 10mb is approx. 2500 pages of text
TASK_PREFIX = 'workers.tasks.'
AUTOMATIC_FILE_WORKFLOW = False


logger = get_task_logger(__name__)


__client = None


def get_client():
    global __client
    if __client is None:
        myclient = DrasticClient(drastic_url)
        res = myclient.authenticate(drastic_user, drastic_password)
        if not res.ok():
            raise IOError("Drastic authentication failed: {0}".format(res.msg()))
        __client = myclient
    return __client


# def get_cdmi_content_stream(path):
#     url = drastic_url + '/api/cdmi/' + path
#     headers = {'Accept-Encoding': 'identity'}
#     resp = requests.get(url, auth=drastic_auth, headers=headers, stream=True)
#     resp.raise_for_status()
#     raw = resp.raw
#     raw.read = functools.partial(resp.raw.read, decode_content=True)
#     if 'content-length' in resp.headers:
#         raw.len = int(resp.headers['content-length'])
#     return raw


# def get_download_content_stream(path):
#     # FIXME this approach doesn't seem to work anymore
#     s = requests.Session()
#     logger.info('get_download_content_stream: checkpoint 0')
#     login = s.get(drastic_url + '/users/login')
#     logger.info('get_download_content_stream: checkpoint 1')
#     login.raise_for_status()
#     logger.info('get_download_content_stream: checkpoint 2')
#     bs = BeautifulSoup(login.text, "lxml")
#     csrfmiddlewaretoken = None
#     for input in bs.select('input'):
#         if input['name'] == 'csrfmiddlewaretoken':
#             csrfmiddlewaretoken = input['value']
#             break
#     data = {'username': drastic_user,
#             'password': drastic_password,
#             'csrfmiddlewaretoken': csrfmiddlewaretoken}
#     logger.info('get_download_content_stream: checkpoint 3')
#     res = s.post(drastic_url + '/users/login', data=data)
#     logger.info('get_download_content_stream: checkpoint 4')
#     res.raise_for_status()
#     logger.info('get_download_content_stream: checkpoint 5')
#     url = drastic_url + '/archive/download' + path
#     headers = {'Accept-Encoding': 'identity'}
#     resp = s.get(url, headers=headers, stream=True)
#     logger.info('get_download_content_stream: checkpoint 6')
#     resp.raise_for_status()
#     logger.info('get_download_content_stream: checkpoint 7')
#     raw = resp.raw
#     raw.read = functools.partial(resp.raw.read, decode_content=True)
#     if 'content-length' in resp.headers:
#         raw.len = int(resp.headers['content-length'])
#     return raw


# def get_cdmi_content(path):
#     url = drastic_url + '/api/cdmi/' + path
#     with closing(requests.get(url, auth=drastic_auth)) as resp:
#         resp.raise_for_status()
#         return resp.content


# def get_download_content(path):
#     s = requests.Session()
#     login = s.get(drastic_url + '/users/login')
#     login.raise_for_status()
#     bs = BeautifulSoup(login.text, "lxml")
#     csrfmiddlewaretoken = None
#     for input in bs.select('input'):
#         if input['name'] == 'csrfmiddlewaretoken':
#             csrfmiddlewaretoken = input['value']
#             break
#     data = {'username': drastic_user,
#             'password': drastic_password,
#             'csrfmiddlewaretoken': csrfmiddlewaretoken}
#     res = s.post(drastic_url + '/users/login', data=data)
#     res.raise_for_status()
#     url = drastic_url + '/archive/download/' + path
#     with closing(s.get(url)) as resp:
#         resp.raise_for_status()
#         return resp.content


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


@app.task(bind=True, default_retry_delay=300, max_retries=10)
def postForExtract(self, path):
    """Post a file to the feature extraction service (DTS)"""
    try:
        # with closing(get_download_content_stream(path)) as stream:
            # POST file to DTS and parse fileid
            url = '{0}/api/extractions/upload_url?commkey={1}'.format(
                clowder_url, clowder_commkey)
            # m = MultipartEncoder(fields={'File': (os.path.basename(path), stream)})
            m = {'fileurl': '{0}{1}'.format(cdmi_proxy_url, path)}
            headers = {
                # 'Content-Type': m.content_type,
                'Accept': 'application/json',
                'Authorization': "Basic {0}".format(clowder_auth_encoded)}
            r = requests.post(url, headers=headers, json=m)
            r.raise_for_status()
            parsed = r.json()
            fileid = parsed['id']
            pollForExtract.apply_async((path, fileid, 1), delay=10)
            add_to_clowder_space.apply_async((path, fileid))
    except IOError as e:
        raise self.retry(exc=e)


@app.task(bind=True, default_retry_delay=300, max_retries=10)
def pollForExtract(self, path, fileid, retries):
    """Poll the feature extraction service for the results of an extraction.
       Re-enqueue this task if still waiting."""
    url = '{0}/api/extractions/{1}/status?commkey={2}'.format(
        clowder_url, fileid, clowder_commkey)
    parsed = None
    try:
        r = requests.get(url)
        r.raise_for_status()
        parsed = r.json()
    except IOError as e:
        raise self.retry(exc=e)

    extractionStatus = parsed['Status']
    doneStatus = ['Done']
    failStatus = ['No Extractor Available. Request is not queued.']
    waitStatus = ['Processing',
                  'Required Extractor is either busy or' +
                  ' is not currently running. Try after some time.']
    if extractionStatus in waitStatus:
        raise self.retry()
    elif extractionStatus in failStatus:
        msg = 'Extract failed for {0} {1} with {2}'.format(path, fileid, extractionStatus)
        logger.warn(msg)
        return
    elif extractionStatus not in doneStatus:
        logger.error('Unrecognized extraction status for {0} {1} with {2}'
                     .format(path, fileid, extractionStatus))
        return

    try:
        # Get existing metadata in Drastic
        res = get_client().get_cdmi(str(path))
        if res.code() in [404, 403]:
            logger.warn("Dropping task for object that gives a 403/403: {0}".format(path))
            return
        if not res.ok():
            raise IOError("Drastic get_cdmi failed: {0}".format(res.msg()))
        cdmi_info = res.json()
        metadata = cdmi_info['metadata']
    except IOError as e:
        raise self.retry(exc=e)

    try:
        # GET new metadata
        url = '{0}/api/files/{1}/metadata.jsonld?commkey={2}'.format(
            clowder_url, fileid, clowder_commkey)
        r = requests.get(url)
        r.raise_for_status()
        parsed = r.json()
        logger.debug("fetched metadata: {0}".format(json.dumps(parsed)))
    except IOError as e:
        raise self.retry(exc=e)

    # GET new tags
    try:
        url2 = '{0}/api/files/{1}/tags?commkey={2}'.format(
            clowder_url, fileid, clowder_commkey)
        r2 = requests.get(url2)
        r2.raise_for_status()
        tags = r2.json()['tags']
        if len(tags) > 0:
            metadata['dts_tags'] = tags
            logger.debug("fetched tags: {0}".format(json.dumps(tags)))

        # Modify existing metadata
        # Create Clowder ID and link field
        metadata['dts_clowder_link'] = '{0}/files/{1}/'.format(clowder_url, fileid)
        metadata['dts_clowder_id'] = fileid
        metadata['dts_metadata'] = parsed

        r = get_client().put(path, metadata=metadata)
        if not r.ok():
            raise IOError(str(r))
    except IOError as e:
        raise self.retry(exc=e)


@app.task(bind=True, default_retry_delay=300, max_retries=5)
def textConversion(self, path):
    """Post a file for conversion to text (DAP)"""
    textLink = None
    logger.info('textConversion: checkpoint 1')
    try:
        # FIXME replace streaming with download_tempfile
        tempfilename = download_tempfile_from_drastic_proxy(path)
        # with closing(get_download_content_stream(path)) as stream:
        logger.info('textConversion: checkpoint 2')
        url = '{0}/convert/txt/?chain=2'.format(dap_url)
        with closing(open(tempfilename, 'rb')) as tempfh:
            m = MultipartEncoder(fields={'file': (os.path.basename(path), tempfh)})
            headers = {
                'Content-Type': m.content_type,
                'Accept': 'text/plain',
                'Authorization': "Basic {0}".format(dap_auth_encoded)}
            r = requests.post(url, headers=headers, data=m)
        logger.info('textConversion: checkpoint 3')
        r.raise_for_status()
        logger.info('textConversion: checkpoint 4')
        textLink = r.text.strip()
        if textLink.endswith('/file/404'):
            logger.info('No text conversion for {0}'.format(path))
            return
        valid = validators.url(textLink)
        logger.info('textConversion: checkpoint 5')
        if not valid:
            logger.warn('Brown Dog returned invalid convert result url: {0}'
                        .format(valid.value))
            return
        logger.info('Got conversion link "{0}" for {1}'
                    .format(textLink, path))
        pollForTextConversion.apply_async(
            (path, textLink), delay=15)
    except IOError as e:
        logger.warn(str(e))
        raise self.retry(exc=e)
    finally:
        os.remove(tempfilename)


@app.task(bind=True, default_retry_delay=300, max_retries=10)
def pollForTextConversion(self, path, link):
    """Tries to download text when available."""
    headers = {'Accept': 'text/plain',
               'Authorization': "Basic {0}".format(dap_auth_encoded)}
    try:
        r = requests.get(link, headers=headers)
        r.raise_for_status()

        res = get_client().get_cdmi(str(path))
        if res.code() in [404, 403]:
            logger.warn("Dropping task for object that gives a 403/403: {0}".format(path))
            return
        if not res.ok():
            raise IOError("Drastic get_cdmi failed: {0}".format(res.msg()))
        cdmi_info = res.json()
        metadata = cdmi_info['metadata']
        metadata['fulltext'] = r.text
        res = get_client().put(path, metadata=metadata)
        if res.code() in [404, 403]:  # object probably deleted
            logger.warn("Dropping task for an object that gives a 403/403: {0}".format(path))
            return
        if not res.ok():
            raise IOError(str(res))
    except IOError as e:
        raise self.retry(exc=e)


@app.task(bind=True, default_retry_delay=300, max_retries=10)
def add_to_clowder_space(self, path, fileid):
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
    try:
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
                app.send_task(TASK_PREFIX+task_name,
                              args=[str(path)+f], kwargs={})
    else:
        for o in cdmi_info[u'children']:
            o = o[:-1] if o.endswith('?') else o
            app.send_task(TASK_PREFIX+task_name,
                          args=[str(path)+o], kwargs={})

    for x in cdmi_info[u'children']:
        x = x[:-1] if x.endswith('?') else x
        if x.endswith('/'):
            traversal.apply_async((str(path)+x, task_name, only_files), queue="traversal")


@app.task(bind=True,
          default_retry_delay=300,
          max_retries=100,
          rate_limit='30/m')
def ingest_httpdir(self, url=None, dest=None):
    """Traverses the file tree under the path given, within the CDMI service.
       Applies the named task to every path."""

    if url is None or dest is None:
        raise Exception("URL and destination path are required")

    app.check_traversal_okay(self)

    # Get directory
    try:
        res = requests.get(url)
        res.raise_for_status()
        dir_info = res.json()

        parsed = urlparse(url)
        dirname = parsed.path.split('/')[-2]
        new_folder_path = dest + dirname + '/'
        logger.info("DIRNAME "+new_folder_path)
        res = get_client().mkdir(new_folder_path)
        if not res.ok():
            raise IOError(str(res))
        logger.info("DIRECTORY INGESTED: "+new_folder_path)

        for f in dir_info:
            if 'file' == f['type']:
                ingest_httpfile.apply_async(
                    args=[str(url)+f['name'], new_folder_path], kwargs=f, delay=10)
            elif 'directory' == f['type']:
                ingest_httpdir.apply_async(args=[],
                                           kwargs={
                                               'url': str(url)+f['name']+'/',
                                               'dest': new_folder_path
                                               }, queue="traversal")
    except IOError as e:
        raise self.retry(exc=e)


@app.task(bind=True, default_retry_delay=300, max_retries=5)
def ingest_httpfile(self, url, dest, name=None, metadata={}, mimetype='application/octet-stream'):
    """Ingests the file at the given URL into Drastic. Files larger than a certain size may be
    treated as a CDMI reference, rather than fully ingested."""
    parsed = urlparse(url)
    if name is None:
        name = basename(parsed.path)
    try:
        tempfilename = download_tempfile(url)
        logger.debug("Downloaded file to: "+tempfilename)
        with closing(open(tempfilename, 'rb')) as f:
            res = get_client().put(dest + name,
                                   f,
                                   metadata=metadata,
                                   mimetype=mimetype)
            if not res.ok():
                raise IOError(str(res))
            cdmi_info = res.json()
            logger.debug("put success for {0}".format(json.dumps(cdmi_info)))
    except IOError as e:
        raise self.retry(exc=e)
    finally:
        os.remove(tempfilename)


SERIES_URL = 'https://catalog.archives.gov/api/v1?naIds={0}'
OBJECTS_URL = ('https://catalog.archives.gov/api/v1?description.fileUnit.parentSeries.naId={0}'
               '&offset={1}&rows={2}&type=object')
NARA_PAGE_SIZE = 200


@app.task(bind=True, default_retry_delay=300, max_retries=5)
def ingest_nara_series(self, naId=None, dest=None, offset=0):
    """Ingests a series into Drastic."""
    if naId is None or dest is None:
        raise Exception("URL and destination path are required")
    app.check_traversal_okay(self)

    # Get series description
    series_json = requests.get(SERIES_URL.format(naId)).json()
    series_descr = series_json['opaResponse']['results']['result'][0]['description']

    # Create folder
    dirname = series_descr['series']['title']
    new_folder_path = dest + dirname + '/'

    # Check if folder exists
    exists_res = get_client().get_cdmi(new_folder_path)
    if exists_res.code() == 404:
        logger.info("Creating base folder in Drastic: "+new_folder_path)
        res = get_client().put_cdmi(new_folder_path, series_descr)
        if not res.ok():
            raise IOError(str(res))
        logger.info("Base folder created: "+new_folder_path)

    # Schedule ingest of page 0
    ingest_nara_series_paged_objects.apply_async(
        args=[],
        kwargs={
            'naId': naId,
            'dest': new_folder_path,
            'offset': offset},
        queue="traversal")


@app.task(bind=True, default_retry_delay=300, max_retries=5)
def ingest_nara_series_paged_objects(self, naId=None, dest=None, offset=0):
    """Ingests a series into Drastic."""
    app.check_traversal_okay(self)

    # Get object descriptions
    try:
        objects_json = requests.get(OBJECTS_URL.format(naId, offset, NARA_PAGE_SIZE)).json()
        object_descrs = objects_json['opaResponse']['results']['result']
        total_objects = objects_json['opaResponse']['results']['total']
        row_count = len(object_descrs)
    except IOError as e:
        raise self.retry(exc=e)

    # Schedule object ingests for this page
    for obj in object_descrs:
        logger.warn(json.dumps(obj))
        file_stuff = obj['objects']['object']['file']
        url = file_stuff['@url']
        mime = file_stuff['@mime']
        name = file_stuff['@name']
        ingest_httpfile.apply_async(
            args=[url, dest],
            kwargs={
                'name': name,
                'mimetype': mime,
                'metadata': obj
            }
        )

    # Schedule next page
    if total_objects > offset + row_count:
        ingest_nara_series_paged_objects.apply_async(
            args=[],
            kwargs={
                'naId': naId,
                'dest': dest,
                'offset': offset + row_count},
            queue="traversal")


def download_tempfile_from_drastic_proxy(path):
    url = '{0}{1}'.format(cdmi_proxy_url, path)
    return download_tempfile(url)


def download_tempfile(url, auth=None):
    from tempfile import NamedTemporaryFile
    tmp = NamedTemporaryFile(delete=False)
    savepath = tmp.name
    tmp.close()
    # NOTE the stream=True parameter
    logger.debug("DOWNLOADING: "+url)
    headers = {'Accept-Encoding': 'identity'}
    with closing(open(savepath, "wb")) as savehandle:
        with closing(requests.get(url, headers=headers, stream=True, auth=auth)) as r:
            r.raise_for_status()
            for chunk in r.iter_content(chunk_size=1024):
                if chunk:  # filter out keep-alive new chunks
                    savehandle.write(chunk)
    return savepath


def stream_from_drastic_proxy(path):
    url = '{0}{1}'.format(cdmi_proxy_url, path)
    return get_httpfile_content_stream(url)


def get_httpfile_content_stream(url):
    headers = {'Accept-Encoding': 'identity'}
    resp = requests.get(url, headers=headers, stream=True)
    resp.raise_for_status()
    raw = resp.raw
    raw.read = functools.partial(resp.raw.read, decode_content=True)
    if 'content-length' in resp.headers:
        raw.len = int(resp.headers['content-length'])
    return raw


def get_tasks():
    result = []
    for t in app.tasks.keys():
        if t.startswith(TASK_PREFIX):
            result.append(t[len(TASK_PREFIX): len(t)])
    return result
