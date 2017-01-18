from __future__ import absolute_import
from celery.utils.log import get_task_logger
from celery import group
from workers.celery_app import app
from workers.util import download_tempfile, get_client
import requests
from urlparse import urlparse
from os.path import basename
import os
import json
from contextlib import closing


logger = get_task_logger(__name__)


@app.task(bind=True,
          default_retry_delay=300,
          max_retries=100,
          rate_limit='30/m')
def ingest_httpdir(self, url=None, dest=None):
    """Ingests the file tree under the path given, using the NGINX JSON directory autoindex."""

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

        file_ingests = []
        folder_ingests = []
        for f in dir_info:
            if 'file' == f['type']:
                s = ingest_httpfile.s(str(url)+f['name'], new_folder_path, metadata=f)
                file_ingests.append(s)
            elif 'directory' == f['type']:
                s = ingest_httpdir.s(url=str(url)+f['name']+'/', dest=new_folder_path)
                folder_ingests.append(s)
        file_job = group(file_ingests)
        file_job.apply_async()
        # result.join()  # wait for files to ingest in parallel
        # file_count += result.completed_count()
        group(folder_ingests).apply_async()
        # for file_c, folder_c in folder_res.get():
        #     file_count += file_c
        #     folder_count += folder_c
        # return (file_count, folder_count)
    except IOError as e:
        raise self.retry(exc=e)


@app.task(bind=True, default_retry_delay=300, max_retries=5)
def ingest_httpfile(self, url, dest, name=None, metadata={}, mimetype='application/octet-stream'):
    """Ingests the file at the given URL into Drastic."""
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
