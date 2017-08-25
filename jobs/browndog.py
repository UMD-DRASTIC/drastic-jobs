""" Workflow jobs for extracting and converting files using the NCSA Brown Dog service.
"""
from __future__ import absolute_import
from jobs.celery_app import app
import requests
import workflow
from requests_toolbelt import MultipartEncoder
import validators
import os
import json
from contextlib import closing
from jobs.util import cdmi_proxy_url, get_client, download_tempfile_from_drastic_proxy
from celery.utils.log import get_task_logger


logger = get_task_logger(__name__)
dap_url = os.getenv("DAP_URL", 'http://localhost:8184')
dap_auth_encoded = os.getenv('DAP_AUTH_ENCODED')
clowder_url = os.getenv('CLOWDER_URL', 'http://localhost:9000')
clowder_auth_encoded = os.getenv('CLOWDER_AUTH_ENCODED')
clowder_commkey = os.getenv('CLOWDER_COMMKEY', 'foo')
clowder_spaceid = os.getenv('CLOWDER_SPACE_ID')


def react(event):
    if 'create' == event.operation and 'resource' == event.object_type:
        postForExtract.apply_async((event.path,))
        if 'text/plain' != event.payload.mimetype:
            textConversion.apply_async((event.path,))


workflow.registry.subscribe(react)


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
