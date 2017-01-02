#!/usr/bin/python
"""Ingest HTTP Directory
Traverses an HTTP directory tree as presented with a JSON autoindex (Nginx), starting from a given folder URL.

Usage:
  ingest_httpdir.py --url=URL --dest=path [--quiet | --verbose]
  ingest_httpdir.py -h | --help

Options:
  --url=URL     Base folder URL to begin traverse (HTTP Index as JSON)
  --verbose     Increase logging output to DEBUG level.
  --quiet       Decrease logging output to WARNING level.
  -h, --help    Show this message.

"""

import logging
from workers.tasks import ingest_httpdir
from docopt import docopt

if __name__ == '__main__':
    arguments = docopt(__doc__, version='Ingest HTTP Directory v1.0')
    print(arguments)
    logger = logging.getLogger("ingest_httpdir")
    sh = logging.StreamHandler()
    logger.addHandler(sh)
    if arguments['--verbose']:
        logger.setLevel(logging.DEBUG)
    elif arguments['--quiet']:
        logger.setLevel(logging.WARNING)
    else:
        logger.setLevel(logging.INFO)

    url = arguments["--url"]
    # file_regex = arguments["FILE_REGEX"] # A regex string or None
    dest = arguments["--dest"]

    DEVNULL = open('/dev/null', 'w')

    if not url.endswith('/'):
        logger.error("URL must be an HTTP folder URL, ending in /")
        exit(1)
    if not dest.endswith('/'):
        logger.error("Destination path must be an existing folder path, ending in /")
        exit(1)
    logger.info('Instructing workers to ingest: {0}'.format(url))

    # Queue traverse job for URL
    ingest_httpdir.apply_async(kwargs={'url': url, 'dest': dest}, queue='traversal')
