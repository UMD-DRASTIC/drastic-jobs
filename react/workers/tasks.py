from __future__ import absolute_import
from workers.celery import app
from celery.utils.log import get_task_logger
import os
from cli.client import IndigoClient
import requests, json
from contextlib import closing
from requests.exceptions import ConnectionError

clowder_url = os.getenv('CLOWDER_URL', 'http://localhost:9000')
clowder_auth_encoded = os.getenv('CLOWDER_AUTH_ENCODED')
clowder_commkey = os.getenv('CLOWDER_COMMKEY', 'foo')
indigo_url = os.getenv('INDIGO_URL', 'http://localhost')
cdmi_proxy_url = os.getenv('CDMI_PROXY_URL', 'http://localhost')
indigo_user = os.getenv('INDIGO_USER', 'worker')
indigo_password = os.getenv('INDIGO_PASSWORD', 'password')
elasticsearch_url = os.getenv('ELASTICSEARCH_URL', 'http://localhost/:9200')
logger = get_task_logger(__name__)

class AuthError(Exception):
    pass

class CDMIError(Exception):
    pass

class ClowderError(Exception):
    pass

class ClowderNoExtractorsError(Exception):
    pass

@app.task
def react(operation, path, stateChange):
    """Reacts to the state changes indicated by parameters, queuing up other jobs"""
    #logger.info('Job launched for: {0} on {1}'.format(operation, path))
    if "create" == operation:
        index.apply_async((path))
        postForExtract.apply_async((path))
    if "update" == operation:
        index.apply_async((path))
    if "delete" == operation:
        deindex.apply_async((path))

@app.task(throws=(AuthError,CDMIError))
def index(path):
    """Reindexes the metadata for a data object"""
    #logger.info('Index job launched for: {0}'.format(path))
    type = 'folder' if str(path).endswith('/') else 'file'
    client = IndigoClient(indigo_url)
    res = client.authenticate(indigo_user, indigo_password)
    if not res.ok():
        logger.error("Failed to authenticate: {0}".format(res.msg()))
        raise AuthError
    res = client.get_cdmi(str(path))
    if not res.ok():
        logger.error('Error : {0}'.format(res.msg()))
        raise CDMIError
    cdmi_info = res.json()
    #logger.debug('CDMI dump: \n {0}'.format(json.dumps(cdmi_info)))
    esdoc = {}
    esdoc['cdmi_path'] = str(path)
    for attr, val in cdmi_info['metadata'].iteritems():
        if attr.startswith(('cdmi_','com.archiveanalytics.indigo_')):
            continue
        esdoc[attr] = val
    url = elasticsearch_url+'/indigo/'+type
    r = requests.post(url, data=json.dumps(esdoc))
    if r.status_code == requests.codes.ok:
        parsed = r.json()
        #logger.info('ES reply: {0}'.format(parsed))
    else:
        logger.error('ES status: {0}'.format(r.status_code))

@app.task
def deindex(path):
    """Removes a data object from the index"""
    logger.info('Deindex job launched for: {0}'.format(path))
    # TODO lookup ID by cdmi_path, just log info and return if not found
    # TODO Remove item from Elasticseaerch

@app.task
def postForExtract(path):
    """Post a file to the feature extraction service (DTS)"""
    #logger.info('Post for extract job launched for: {0}'.format(path))

    # Open Indigo stream
    client = IndigoClient(indigo_url)
    authrq = client.authenticate(indigo_user, indigo_password)
    if not authrq.ok():
        logger.error("Failed to authenticate: {0}".format(authrq.msg()))
        raise AuthError
    try:
        with closing(client.open(str(path))) as resp:
            if resp.status_code == 404:
                logger.error('Error : {0}'.format(resp.msg()))
                raise CDMIError

            # POST file to DTS and parse fileid
            #fileurl = '{0}/{1}'.format(cdmi_proxy_url, path)
            #data = {}
            #data['fileurl'] = fileurl
            url = '{0}/api/extractions/upload_file?commkey={1}'.format(clowder_url, clowder_commkey)
            #logger.debug("sending to url: {0},\nwith data: {1}".format( url, json.dumps(data) ) )
            files = [('File', (os.path.basename(path), resp.raw, 'application/octet-stream'))]
            headers = { 'Accept': 'application/json', 'Authorization': "Basic {0}".format(clowder_auth_encoded)}
            #r = requests.post(url, headers=headers, files={'File' : (os.path.basename(path), resp.raw)})
            r = requests.post(url, headers=headers, files=files)
            if r.status_code == requests.codes.ok:
                parsed = r.json()
                fileid = parsed['id']
                pollForExtract.apply_async((path, fileid), countdown=1)
            else:
                logger.warn('Post for extract failed for {0} with {1} {2}'.format(path, r.status_code, r.text))
    except ConnectionError as e:
        logger.error("Connection failed: {0}".format(e))
        raise CDMIError

@app.task
def pollForExtract(path, fileid):
    """Poll the feature extraction service for the results of an extraction. Re-enqueue this task if still waiting."""
    #logger.info('Poll for extract job launched for: {0} with fileid {1}'.format(path, fileid))
    # See if extract has succeeded or current status
    url = '{0}/api/extractions/{1}/status?commkey={2}'.format(clowder_url, fileid, clowder_commkey)
    r = requests.get(url)
    if r.status_code != requests.codes.ok:
        logger.error('Failed to poll for extract for {0} {1} with {2} {3}'.format(path, fileid, r.status_code, r.text))
        return
    parsed = r.json()
    #logger.debug("got poll JSON: {0}".format(json.dumps(parsed)))
    extractionStatus = parsed['Status']
    doneStatus = ['Done']
    failStatus = ['Failed','No Extractor Available. Request is not queued.'] # NOTE 'Failed' is a placeholder
    waitStatus = ['Processing','Required Extractor is either busy or is not currently running. Try after some time.']
    if extractionStatus in doneStatus:
        fetchUpdatesFromExtract.apply_async((path, fileid))
    elif extractionStatus in waitStatus:
        pollForExtract.apply_async((path, fileid), countdown=10)
    elif extractionStatus in failStatus:
        logger.warn('Extract failed for {0} {1} with {2}'.format(path, fileid, extractionStatus))
        raise ClowderNoExtractorsError
    else:
        logger.error('Unrecognized extraction status for {0} {1} with {2}'.format(path, fileid, extractionStatus))
        raise ClowderError

@app.task
def fetchUpdatesFromExtract(path, fileid):
    """Fetch the feature extraction results and update repository."""
    #logger.info('Fetch updates from extract job launched for: {0} with fileid {1}'.format(path, fileid))
    # TODO GET new metadata
    url = '{0}/api/files/{1}/tags?commkey={2}'.format(clowder_url, fileid, clowder_commkey)
    r = requests.get(url)
    if r.status_code != requests.codes.ok:
        logger.error('Failed to poll for extract for {0} {1} with {2} {3}'.format(path, fileid, r.status_code, r.text))
        return
    parsed = r.json()
    logger.debug("fetched tags: {0}".format(json.dumps(parsed)))

    url = '{0}/api/files/{1}/metadata?commkey={2}'.format(clowder_url, fileid, clowder_commkey)
    r = requests.get(url)
    if r.status_code != requests.codes.ok:
        logger.error('Failed to poll for extract for {0} {1} with {2} {3}'.format(path, fileid, r.status_code, r.text))
        return
    parsed = r.json()
    logger.debug("fetched metadata: {0}".format(json.dumps(parsed)))
    #extractionStatus = parsed['Status']

    # TODO POST new metadata in Indigo
    client = IndigoClient(indigo_url)
    authrq = client.authenticate(indigo_user, indigo_password)
    if not authrq.ok():
        logger.error("Failed to authenticate: {0}".format(authrq.msg()))
        raise AuthError
    res = client.get_cdmi(str(path))
    if not res.ok():
        logger.info('Failed to get CDMI metadata: {0}'.format(res.msg()))
        raise CDMIError
    metadata = res.json()['metadata']

    for attr, val in parsed.iteritems():
        if attr not in ['authorId', 'date-created']:
            metadata[attr] = val

    res = client.put(path, metadata=metadata)
    if not res.ok():
        logger.info('Error putting metadata: {0}'.format(res.msg()))
        raise CDMIError
    index.apply_async((path))

@app.task(throws=(AuthError))
def traversal(path, task_name, only_files):
    """Traverses the file tree under the path given, within the CDMI service. Applies the named task to every path."""
    #logger.info("Traversing {1} at: {0}".format(path, task_name))
    client = IndigoClient(indigo_url)
    res = client.authenticate(indigo_user, indigo_password)
    if not res.ok():
        logger.error("Failed to authenticate: {0}".format(res.msg()))
        raise AuthError
    res = client.ls(path)
    if not res.ok():
        logger.error("CDMI 'ls' request failed: {0} at {1}".format(res.msg(), path))
        return

    cdmi_info = res.json()
    if not cdmi_info[u'objectType'] == u'application/cdmi-container':
        logger.error("Cannot traverse a file path: {0}".format(path))
        return

    if only_files:
        for f in cdmi_info[u'children']:
            if not f.endswith('/'):
                app.send_task('workers.tasks.'+task_name, args=[str(path)+f], kwargs={})
    else:
        for o in cdmi_info[u'children']:
            app.send_task('workers.tasks.'+task_name, args=[str(path)+o], kwargs={})

    for x in cdmi_info[u'children']:
        if x.endswith('/'):
            traversal.apply_async((str(path)+x,task_name, only_files))
