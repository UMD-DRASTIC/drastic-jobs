from celery.utils.log import get_task_logger
from celery import group, chord
from jobs.celery_app import app
from jobs.util import download_tempfile, get_client
import requests
from urllib.parse import urlparse, quote as urlquote
from os.path import basename
import os
import json
import sys
import time
from contextlib import closing

logger = get_task_logger(__name__)


@app.task(bind=True, default_retry_delay=300, rate_limit='30/m')
def batch_ingest_httpdir(self, url=None, dest=None):
    """Batches the folders and files under the path given, using the NGINX JSON directory
    autoindex."""

    epoch_start = int(time.time())
    # Create top folder in Drastic
    res = requests.get(url)
    res.raise_for_status()
    dirname = urlparse(url).path.split('/')[-2]
    batch_dir = os.path.join(dest, dirname) + '/'
    res = get_client().mkdir(batch_dir)
    if not res.ok():
        raise IOError('Cannot make folder {0}: {1}'.format(batch_dir, str(res)))
    logger.info(u"Batch ingest starting: "+batch_dir)

    # Schedule a recursive count, then record it in Drastic metadata
    (file_cnt, file_byte_cnt, folder_cnt) = count_httpdir(url=url)
    logger.info(u"Batch count complete, {0} files, {1} bytes.".format(file_cnt, file_byte_cnt))
    record_batch_count(file_cnt, file_byte_cnt, folder_cnt, epoch_start, batch_dir)

    mkdirs = mkdirs_httpdir.si(url, batch_dir)  # batch_dir /NARA/RG .....
    fc = folders_complete.si(folder_cnt, batch_dir)
    ingest = ingest_files.si(url, batch_dir)
    (mkdirs | fc | ingest).apply_async()


def iter_httpdir(url, files=True, folders=True, parentPath=''):
    """Yields the folder and/or file records *under* the URL given, using the NGINX JSON directory
    autoindex. Yeilds tuples: (filename, parentdir, escaped URL)"""
    logger.debug(u'iter: {0}'.format(url))
    res = requests.get(url)
    try:
        res.raise_for_status()
    except IOError as e:
        logger.warn(u"Unable to descend into HTTP folder {0}, due to error {1}".format(url, e))
        return
    try:
        dir_info = res.json()
    except:
        logger.warn(u"Unable to descend into HTTP folder {0}, due to error {1}"
                    .format(url, sys.exc_info()[0]))
        return
    for f in dir_info:
        name = str(f['name'])
        url_name = urlquote(name.encode('utf-8'))
        if 'file' == f['type']:
            if files:
                furl = u'{0}{1}'.format(url, url_name)
                yield (f, parentPath, furl)
        elif 'directory' == f['type']:
            nexturl = u'{0}{1}/'.format(url, url_name)
            if folders:
                yield (f, parentPath, nexturl)
            nextParentPath = os.path.join(parentPath, name)
            logger.debug(
                u'iter: nexturl:{0} nextParentPath:{1} parentPath:{2}'.format(nexturl,
                                                                              nextParentPath,
                                                                              parentPath))
            for foo in iter_httpdir(nexturl,
                                    files=files,
                                    folders=folders,
                                    parentPath=nextParentPath):
                yield foo


@app.task(default_retry_delay=300)
def count_httpdir(url):
    """Counts the folders and files under the path given, using the NGINX JSON directory
    autoindex."""
    file_cnt = 0
    file_byte_cnt = 0
    folder_cnt = 0
    for (f, parentPath, furl) in iter_httpdir(url):
        if f['type'] == 'file':
            file_cnt += 1
            file_byte_cnt += f['size']
        elif f['type'] == 'directory':
            folder_cnt += 1
    return (file_cnt, file_byte_cnt, folder_cnt)


@app.task(default_retry_delay=300, rate_limit='30/m')
def mkdirs_httpdir(url, batch_dir):
    """Counts the folders and files under the path given, using the NGINX JSON directory
    autoindex."""
    count = 0
    notifyCount = 20
    for (f, parentPath, furl) in iter_httpdir(url, files=False):
        name = str(f['name'])
        new_folder_path = os.path.join(batch_dir, parentPath, name) + '/'
        logger.debug(u'new_folder_path: {0}'.format(new_folder_path))
        res = get_client().mkdir(new_folder_path)
        if not res.ok():
            logger.error(u'Cannot make directory: {0}'.format(new_folder_path))
            continue
        count += 1
        if count >= notifyCount:
            incr_batch_progress.s(batch_dir, folder_cnt=count).apply_async()
            count = 0
    incr_batch_progress.s(batch_dir, folder_cnt=count).apply_async()


@app.task(default_retry_delay=300, rate_limit='30/m')
def ingest_files(url, batch_dir):
    """Ingests the files under the path given, placing them in the same folder structure within
    the batch_dir folder in Drastic."""
    if url is None:
        raise Exception("URL and destination path are required")
    queue = []
    queueBytes = 0
    groupCount = 10
    for (f, parentPath, furl) in iter_httpdir(url, folders=False):
        dest = os.path.join(batch_dir, parentPath)
        logger.debug(u'furl: {0} || dest: {1} || batch_dir: {2} || parentPath: {3}'.format(
            furl, dest, batch_dir, parentPath))
        s = ingest_httpfile.si(furl, dest)
        queue.append(s)
        queueBytes += f['size']
        if len(queue) >= groupCount:
            chord(queue)(incr_batch_progress.si(
                batch_dir, file_cnt=len(queue), file_bytes_cnt=queueBytes))
            queue = []
    if len(queue) > 0:
        chord(queue)(incr_batch_progress.si(
            batch_dir, file_cnt=len(queue), file_bytes_cnt=queueBytes, done=True))
    else:
        incr_batch_progress.si(
            batch_dir, done=True)


@app.task
def record_batch_count(file_cnt, file_bytes_cnt, folder_cnt, epoch_start, batch_dir):
    # Get existing metadata in Drastic
    res = get_client().ls(batch_dir)
    if not res.ok():
        raise IOError("Drastic get_cdmi failed: {0}".format(res.msg()))
    metadata = res.json()['metadata']
    metadata['batch_file_count'] = file_cnt
    metadata['batch_file_bytes_count'] = file_bytes_cnt
    metadata['batch_folder_count'] = folder_cnt
    metadata['batch_epoch_start'] = epoch_start
    metadata['batch_state'] = 'ingesting'
    metadata['batch_file_progress'] = 0
    metadata['batch_file_bytes_progress'] = 0
    metadata['batch_folder_progress'] = 0
    r = get_client().put(batch_dir, metadata=metadata)
    if not r.ok():
        raise IOError(str(r))


@app.task
def folders_complete(folder_cnt, batch_dir):
    logger.info(u"Folders created for batch: {0}".format(batch_dir))
    # Get existing metadata in Drastic
    res = get_client().get_cdmi(batch_dir)
    if not res.ok():
        raise IOError("Drastic get_cdmi failed: {0}".format(res.msg()))
    metadata = res.json()['metadata']
    metadata['batch_folder_progress'] = folder_cnt
    r = get_client().put(batch_dir, metadata=metadata)
    if not r.ok():
        raise IOError(str(r))


@app.task
def incr_batch_progress(batch_dir, file_cnt=0, file_bytes_cnt=0, folder_cnt=0, done=False):
    # Get existing metadata in Drastic
    res = get_client().get_cdmi(batch_dir)
    if not res.ok():
        raise IOError("Drastic get_cdmi failed: {0}".format(res.msg()))
    metadata = res.json()['metadata']
    progress_file_old = metadata.get('batch_file_progress', 0)
    progress_file = file_cnt + int(progress_file_old)
    progress_file_bytes_old = metadata.get('batch_file__bytes_progress', 0)
    progress_file_bytes = file_bytes_cnt + int(progress_file_bytes_old)
    progress_folder_old = metadata.get('batch_folder_progress', 0)
    progress_folder = folder_cnt + int(progress_folder_old)
    metadata['batch_file_progress'] = progress_file
    metadata['batch_file_bytes_progress'] = progress_file_bytes
    metadata['batch_folder_progress'] = progress_folder
    if done:
        metadata['batch_state'] = 'done'
        metadata['batch_epoch_end'] = int(time.time())
    r = get_client().put(batch_dir, metadata=metadata)
    if not r.ok():
        raise IOError(str(r))


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
        logger.debug(u"DIRNAME "+new_folder_path)
        res = get_client().mkdir(new_folder_path)
        if not res.ok():
            raise IOError(str(res))
        logger.debug(u"DIRECTORY INGESTED: "+new_folder_path)

        file_ingests = []
        folder_ingests = []
        for f in dir_info:
            if 'file' == f['type']:
                s = ingest_httpfile.s(str(url)+f['name'], new_folder_path)
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
def ingest_httpfile(self, url, destPath, name=None):
    """Ingests the file at the given URL into Drastic."""
    parsed = urlparse(url)
    if name is None:
        name = basename(parsed.path)
    name = name.replace('&', '_')
    tempfilename = None
    try:
        tempfilename = download_tempfile(url)
    except IOError as e:
        os.remove(tempfilename)
        raise self.retry(exc=e)
    try:
        logger.debug(u"Downloaded file to: "+tempfilename)
        with closing(open(tempfilename, 'rb')) as f:
            res = get_client().put(destPath+'/'+name, f)
            if res.code() in [406, 999]:
                return
            if not res.ok():
                raise IOError('Failed to put {} to {}. Got {} {}'.format(
                    tempfilename, destPath+'/'+name, res.code(), res.msg()))
            logger.debug(u"put success for {0}".format(destPath+name))
    finally:
        os.remove(tempfilename)
