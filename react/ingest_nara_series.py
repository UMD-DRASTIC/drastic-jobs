#!/usr/bin/python
import docopt
import logging
from workers.tasks import ingest_nara_series

usage = """Ingest All Objects in a NARA Catalog Series
This program gathers lists of objects from the NARA Catalog API, based on parent series. It creates
a series folder with metadata at the specified Drastic repository path. It then downloads all the
objects and their metadata into the Drastic series folder. The functionality is split into async
tasks that run in the CIBER worker environment.

Usage:
  ingest_nara_series.py --naId=series-ID --dest=path --offset=num [--quiet | --verbose]
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
    offset = ret['--offset']
    if not dest.endswith('/'):
        logger.error("Destination path must be an existing folder path, ending in /")
        exit(1)
    logger.info('Instructing workers to ingest NARA series: {0}'.format(naId))

    if offset is None:
        offset = 0

    # Queue traverse job for URL
    ingest_nara_series.apply_async(kwargs={'naId': naId, 'dest': dest, 'offset': offset},
                                   queue='traversal')
    return 0

main()
