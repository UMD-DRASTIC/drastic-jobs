from __future__ import absolute_import
from jobs.celery_app import app
from jobs.httpdir import ingest_httpfile
from jobs.util import get_client
import requests
import json
from celery.utils.log import get_task_logger
from celery import group, chord


logger = get_task_logger(__name__)
SERIES_URL = 'https://catalog.archives.gov/api/v1?naIds={0}'
NARA_PAGE_SIZE = 20


@app.task(bind=True, default_retry_delay=300, max_retries=5)
def ingest_series(self, naId=None, dest=None, offset=0):
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

    # Schedule page 0
    schedule_page.s([],
                    naId=naId,
                    dest=new_folder_path,
                    offset=offset).apply_async()


@app.task(bind=True, default_retry_delay=300, max_retries=5)
def schedule_page(self, newresults, oldresults=0, naId=None, dest=None, offset=0):
    """Ingests a series into Drastic."""
    app.check_traversal_okay(self)

    OBJECTS_URL = ('https://catalog.archives.gov/api/v1?description.fileUnit.parentSeries.naId={0}'
                   '&offset={1}&rows={2}&type=object')

    newcount = len(newresults)
    logger.warn('{0} nara objects just ingested'.format(newcount))

    # Get object descriptions
    try:
        objects_json = requests.get(OBJECTS_URL.format(naId, offset, NARA_PAGE_SIZE)).json()
        object_descrs = objects_json['opaResponse']['results']['result']
        total_objects = objects_json['opaResponse']['results']['total']
        row_count = len(object_descrs)
    except IOError as e:
        raise self.retry(exc=e)

    # Schedule object ingests for this page
    page_tasks = []
    for obj in object_descrs:
        # logger.warn(json.dumps(obj))
        file_stuff = obj['objects']['object']['file']
        idnum = obj['objects']['object']['@id']
        url = file_stuff['@url']
        mime = file_stuff['@mime']
        name = str(idnum) + '_' + file_stuff['@name']
        s = ingest_httpfile.s(url, dest, name=name, mimetype=mime, metadata=obj)
        page_tasks.append(s)
    page_job = group(page_tasks)
    logger.warn('scheduling {0} ingests at nara offset {1}'.format(row_count, offset))

    # Schedule next page
    if total_objects > offset + row_count:
        next_page = schedule_page.s(naId=naId,
                                    dest=dest,
                                    offset=offset + row_count,
                                    oldresults=oldresults+newcount)
        chord(page_job)(next_page)
        return oldresults + newcount
    else:
        page_job.apply_async()
        return oldresults + newcount + len(page_tasks)
