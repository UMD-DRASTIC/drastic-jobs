#!/usr/bin/python
from __future__ import absolute_import
import docopt
import logging
from workers.nara import ingest_series

usage = """Ingest All Objects in a NARA Catalog Series
This program gathers lists of objects from the NARA Catalog API, based on parent series. It creates
a series folder with metadata at the specified Drastic repository path. It then downloads all the
objects and their metadata into the Drastic series folder. The functionality is split into async
tasks that run in the CIBER worker environment.

Usage:
  ingest_nara_series.py --naId=series-ID --dest=path [--offset=num] [--quiet | --verbose]
  ingest_nara_series.py -h | --help

Options:
  --naId=series-ID     NARA Catalog series identity number
  --dest=path          Path in Drastic Catalog
  -h, --help           Show this message.

"""


def main():
    ret = docopt.docopt(usage)
    logger = logging.getLogger("ingest_nara_series")
    sh = logging.StreamHandler()
    logger.addHandler(sh)
    dest = ret['--dest']
    naId = ret['--naId']
    offset = 0
    if ret['--offset'] is not None:
        offset = int(ret['--offset'])
    if not dest.endswith('/'):
        logger.error("Destination path must be an existing folder path, ending in /")
        exit(1)

    # Queue traverse job for URL
    job = ingest_series.s(naId=naId, dest=dest, offset=offset)
    logger.warn('Instructing workers to ingest NARA series: \n{0}'.format(str(job)))
    result = job.apply_async()
    print('Ingest task ID: {0}'.format(result.id))
    return 0

main()
