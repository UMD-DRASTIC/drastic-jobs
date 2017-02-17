#!/usr/bin/python
"""Ingest HTTP Directory
Traverses an HTTP directory tree as presented with a JSON autoindex (Nginx), starting from a given folder URL.

Usage:
  count_httpdir.py --url=URL [--quiet | --verbose]
  count_httpdir.py -h | --help

Options:
  --url=URL     Base folder URL to begin traverse (HTTP Index as JSON)
  --verbose     Increase logging output to DEBUG level.
  --quiet       Decrease logging output to WARNING level.
  -h, --help    Show this message.

"""

from __future__ import absolute_import
import logging
from jobs.httpdir import count_httpdir
from docopt import docopt


if __name__ == '__main__':
    arguments = docopt(__doc__, version='Ingest HTTP Directory v1.0')
    print(arguments)
    logger = logging.getLogger("count_httpdir")
    sh = logging.StreamHandler()
    logger.addHandler(sh)
    if arguments['--verbose']:
        logger.setLevel(logging.DEBUG)
    elif arguments['--quiet']:
        logger.setLevel(logging.WARNING)
    else:
        logger.setLevel(logging.INFO)

    url = arguments["--url"]

    # DEVNULL = open('/dev/null', 'w')

    if not url.endswith('/'):
        logger.error("URL must be an HTTP folder URL, ending in /")
        exit(1)
    logger.info('Instructing workers to ingest: {0}'.format(url))

    # Queue traverse job for URL
    result = count_httpdir.s(url=url).apply_async()
    print('Ingest task ID: {0}\n{1}'.format(result.id, result.info))
    exit(0)
