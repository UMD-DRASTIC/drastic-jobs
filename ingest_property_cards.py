#!/usr/bin/python
import docopt
import logging
from jobs.nara import ingest_property_cards

usage = """Ingest All Property Cards from NARA WWII Restitution Records
This program gathers lists of objects from the NARA Catalog API, based on parent series. It creates
a series folder with metadata at the specified Drastic repository path. It then downloads all the
objects and their metadata into the Drastic series folder. The functionality is split into async
tasks that run in the CIBER worker environment.

Usage:
  ingest_nara_series.py --dest=path [--quiet | --verbose]
  ingest_nara_series.py -h | --help

Options:
  --dest=path          Path in Drastic Catalog
  -h, --help           Show this message.

"""


def main():
    ret = docopt.docopt(usage)
    logger = logging.getLogger("ingest_property_cards")
    sh = logging.StreamHandler()
    logger.addHandler(sh)
    dest = ret['--dest']
    if not dest.endswith('/'):
        logger.error("Destination path must be an existing folder path, ending in /")
        exit(1)

    # Queue traverse job for URL
    job = ingest_property_cards.s(dest=dest)
    logger.warn('Instructing workers to ingest NARA property cards: \n{0}'.format(str(job)))
    result = job.apply_async()
    print('Ingest task ID: {0}'.format(result.id))
    exit(0)

main()
