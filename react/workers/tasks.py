from __future__ import absolute_import
from workers.celery import app
from celery.utils.log import get_task_logger
import os

clowder_url = os.getenv('CLOWDER_URL', 'http://localhost:9000')
cdmi_proxy_url = os.getenv('CDMI_PROXY_URL', 'http://localhost/ciber/api/cdmi')
logger = get_task_logger(__name__)

@app.task
def react(operation, path, stateChange):
    """Reacts to the state changes indicated by parameters, queuing up other jobs"""
    logger.info('Job launched for: {0} on {1}'.format(operation, path))
    if "create" == operation:
        index.apply_async((path, stateChange))
        postForExtract.apply_async((path))
    if "update" == operation:
        index.apply_async((path, stateChange))
    if "delete" == operation:
        deindex.apply_async((path))

@app.task
def index(path, stateChanged):
    """Reindexes the metadata for a data object"""
    logger.info('Index job launched for: {0} on {1}'.format(path, stateChanged))
    # NOTE It is possible that Clowder can talk directly to elasticsearch. We should see how well this works too..
    # TODO GET metadata from Indigo or stageChanged
    # TODO POST fields to Elasticseaerch

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
    url = clowder_url+'/api/extractions/uploadByURL'
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
    url = clowder_url+'/api/extractions/{0}/status'.format(fileid)
    r = requests.get(url)
    if r.status_code != requests.codes.ok:
        logger.error('Failed to poll for extract for {0} {1} with {2} {3}'.format(path, fileid, r.status_code, r.text))
        return
    parsed = r.json()
    extractionStatus = parsed['status']
    doneStatus = ['Done']
    failStatus = ['Failed'] # NOTE 'Failed' is a placeholder
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
