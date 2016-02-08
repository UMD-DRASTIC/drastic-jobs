from __future__ import absolute_import
from workers.celery import app
from celery.utils.log import get_task_logger
import os
from cli.client import IndigoClient

clowder_url = os.getenv('CLOWDER_URL', 'http://localhost:9000')
cdmi_proxy_url = os.getenv('CDMI_PROXY_URL', 'http://localhost/ciber/api/cdmi')
elasticsearch_url = os.getenv('ELASTICSEARCH_URL', 'http://localhost/:9200')
logger = get_task_logger(__name__)

def get_client(self):
    client = IndigoClient(cdmi_proxy_url)
    return client

@app.task
def react(operation, path, stateChange):
    """Reacts to the state changes indicated by parameters, queuing up other jobs"""
    logger.info('Job launched for: {0} on {1}'.format(operation, path))
    if "create" == operation:
        index.apply_async((path, stateChange))
        postForExtract.apply_async((path))
    if "update" == operation:
        index.apply_async((path))
    if "delete" == operation:
        deindex.apply_async((path))

@app.task
def index(path):
    """Reindexes the metadata for a data object"""
    logger.info('Index job launched for: {0} on {1}'.format(path, stateChanged))
    type = 'folder' if path.endswith('/') else 'file'
    client = self.get_client()
    res = client.get_cdmi(path)
    if not res.ok():
        logger.error('Error : {0} {1}'.format(r.code(), r.msg()))
    cdmi_info = res.json()
    logger.info('CDMI dump: \n'+json.dumps(cdmi_info))
    data = cdmi_info
    url = elasticsearch_url+'/indigo/'+type
    r = requests.post(url,data = data)
    if r.status_code == requests.codes.ok:
        parsed = r.json()
        logger.info('ES reply: {0}'.format(parsed))
    else:
        logger.error('ES status: {0}'.format(r.status_code))

@app.task
def deindex(path):
    """Removes a data object from the index"""
    logger.info('Deindex job launched for: {0}'.format(path))
    # TODO Remove item from Elasticseaerch

@app.task
def postForExtract(path):
    """Post a file to the feature extraction service (DTS)"""
    logger.info('Post for extract job launched for: {0}'.format(path))
    # POST file to DTS and parse fileid
    data = {}
    data['fileurl'] = cdmi_proxy_url + path
    url = clowder_url+'/api/extractions/upload_url?commkey='
    r = requests.post(url,data = data)
    if r.status_code == requests.codes.ok:
        parsed = r.json()
        fileid = parsed['fileid']
        pollForExtract.apply_async((path, fileid))
    else:
        logger.warn('Post for extract failed for {0} with {1} {2}'.format(path, r.status_code, r.text))

@app.task
def pollForExtract(path, fileid):
    """Poll the feature extraction service for the results of an extraction. Re-enqueue this task if still waiting."""
    logger.info('Poll for extract job launched for: {0} with fileid {1}'.format(path, fileid))
    # See if extract has succeeded or current status
    url = '{0}/api/extractions/{1}/status?commkey={2}'.format(clowder_url, fileid, commkey)
    r = requests.get(url)
    if r.status_code != requests.codes.ok:
        logger.error('Failed to poll for extract for {0} {1} with {2} {3}'.format(path, fileid, r.status_code, r.text))
        return
    parsed = r.json()
    extractionStatus = parsed['status']
    doneStatus = ['Done']
    failStatus = ['Failed','No Extractor Available. Request is not queued.'] # NOTE 'Failed' is a placeholder
    waitStatus = ['Processing','Required Extractor is either busy or is not currently running. Try after some time.']
    if extractionStatus in doneStatus:
        fetchUpdatesFromExtract.apply_async((path, fileid))
    elif extractionStatus in waitStatus:
        pollForExtract.apply_async((path, fileid))
    elif extractionStatus in failStatus:
        logger.warn('Extract failed for {0} {1} with {2}'.format(path, fileid, extractionStatus))
    else:
        logger.error('Unrecognized extraction status for {0} {1} with {2}'.format(path, fileid, extractionStatus))

@app.task
def fetchUpdatesFromExtract(path, fileid):
    """Fetch the feature extraction results and update repository."""
    logger.info('Fetch updates from extract job launched for: {0} with fileid {1}'.format(path, fileid))
    # TODO GET new metadata
    # TODO POST new metadata in Indigo

@app.task
def traverse(path, task_name):
    """Traverses the file tree under the path given, within the CDMI service. Applies the named task to every path."""
    # TODO GET listing for path
    # TODO Queue the named task for current node
    # TODO Queue traverse task for any child nodes
