#!/usr/bin/python
"""Traverse Drastic
Traverses the Drastic repository tree, starting from a given folder path.

Usage:
  traverse.py --path=PATH [--task=NAME] [--only-files] [--quiet | --verbose]
  traverse.py -h | --help

Options:
  --path=PATH   Base folder to begin traverse (under CDMI endpoint)
  --task=NAME   Name of a task to apply to every path [default: index]
  --only-files  Only run the task on file paths
  --verbose     Increase logging output to DEBUG level.
  --quiet       Decrease logging output to WARNING level.
  -h, --help    Show this message.

Available Tasks:
{0}
"""

import logging
from jobs.celery_app import app
from jobs.workflow import traversal
from docopt import docopt

if __name__ == '__main__':
    usage = __doc__.format('\n'.join([t for t in app.tasks.keys()]))
    arguments = docopt(usage, version='Traverse v1.0')
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

    path = arguments["--path"]
    # file_regex = arguments["FILE_REGEX"] # A regex string or None
    task_name = arguments["--task"]
    only_files = True if arguments['--only-files'] else False

    DEVNULL = open('/dev/null', 'w')

    if not path.endswith('/'):
        logger.error("Path must be a folder path, ending in /")
        exit(1)
    logger.info('Instructing workers to traverse: {0}'.format(path))

    # Queue traverse job for URL
    result = traversal.s(path, task_name, only_files).apply_async()
    print('Ingest task ID: {0}'.format(result.id))

    exit(0)
