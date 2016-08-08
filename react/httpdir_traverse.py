#!/usr/bin/python
"""HTTP Directory Traverse
Traverses an HTTP directory tree as presented with a JSON autoindex (Nginx), starting from a given folder URL.

Usage:
  traverse.py --url=URL [--task=NAME] [--only-files] [--quiet | --verbose]
  traverse.py -h | --help

Options:
  --url=URL     Base folder URL to begin traverse (HTTP Index as JSON)
  --task=NAME   Name of a task to apply to every path [default: index]
  --only-files  Only run the task on file paths
  --verbose     Increase logging output to DEBUG level.
  --quiet       Decrease logging output to WARNING level.
  -h, --help    Show this message.

"""

import json
import logging
from workers.celery import app
from workers.tasks import traverse_httpdir
from docopt import docopt

if __name__ == '__main__':
    arguments = docopt(__doc__, version='HTTP Directory Traverse v1.0')
    print(arguments)
    logger = logging.getLogger("traverse")
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
    task_name = arguments["--task"]
    only_files = True if arguments['--only-files'] else False

    DEVNULL = open('/dev/null', 'w')

    if not url.endswith('/'):
        logger.error("URL must be an HTTP folder URL, ending in /")
        exit(1)
    logger.info('Instructing workers to traverse: {0}'.format(url))

    # Queue traverse job for URL
    traverse_httpdir.apply_async((url, task_name, only_files))
