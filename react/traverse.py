"""Listener for Reacting to Indigo
Listens for Indigo CRUD events over MQTT and creates a delayed celery task to analyze and
react to those events as compute resources allow it. By default this program listens for
changes anywhere in the Indigo hierarchy. However, you may instead supply a list of
paths that you want watched.

Usage:
  traverse.py [--path=<base path>] [--task=<task name>] [--quiet | --verbose] [FILE_REGEX]
  traverse.py -h | --help

Options:
  --url       Base path to traverse (under CDMI endpoint) [default: /]
  --task      Name of a celery task to apply to every path [default: react]
  -h, --help  Show this message.
  --verbose   Increase logging output to DEBUG level.
  --quiet     Decrease logging output to WARNING level.

"""

import json
import logging
from workers.celery import app
from workers.tasks import react
from docopt import docopt

if __name__ == '__main__':
    arguments = docopt(__doc__, version='Listener v1.0')

    logger = logging.getLogger("listener")
    fh = logging.FileHandler('listener.log')
    logger.addHandler(fh)
    if arguments['--verbose']:
        logger.setLevel(logging.DEBUG)
    elif arguments['--quiet']:
        logger.setLevel(logging.WARNING)
    else:
        logger.setLevel(logging.INFO)

    cdmi_url = arguments["--url"]
    file_regex = arguments["FILE_REGEX"] # A regex string or None
    task_name = arguments["--task"]

    DEVNULL = open('/dev/null', 'w')

    logger.debug('Instructing workers to traverse: {0}'.format(cdmi_url))

    # Queue traverse job for URL
    traverse.apply_async((path, task_name))
