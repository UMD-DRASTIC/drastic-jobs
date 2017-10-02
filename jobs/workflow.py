"""This module defines asyncronous tasks for CI-BER workflow"""
from jobs.celery_app import app
from celery.utils.log import get_task_logger
from jobs.util import get_client
import json
import re

logger = get_task_logger(__name__)


class Event(object):
    pass


class Observable(object):
    """Simple observer pattern that will call back to registered listeners"""
    def __init__(self):
        self.callbacks = []

    def subscribe(self, callback):
        self.callbacks.append(callback)

    def fire(self, **attrs):
        e = Event()
        e.source = self
        for k, v in attrs.iteritems():
            setattr(e, k, v)
        for fn in self.callbacks:
            fn(e)


# Listeners can subscribe to workflow events
registry = Observable()


@app.task
def react(operation, object_type, path, uuid, state_change):
    """Responds to state change messages, firing events to registered listeners"""
    path = path[:-1] if path.endswith('?') else path
    registry.fire(operation=operation, object_type=object_type, path=path, uuid=uuid,
                  state_change=state_change)


@app.task(bind=True,
          default_retry_delay=300,
          max_retries=100,
          rate_limit='30/m')
def traversal(self, path, task_name, only_files, include_pattern=None):
    """Traverses the file tree under the path given, within the CDMI service.
       Applies the named task to every path."""

    app.check_traversal_okay(self)

    path = path[:-1] if path.endswith('?') else path

    try:
        res = get_client().ls(path)
        if res.code() in [404, 403]:  # object probably deleted
            logger.warn("Dropping task for an object that gives a 403/403: {0}".format(path))
            return
        if not res.ok():
            raise IOError(str(res))
    except IOError as e:
        raise self.retry(exc=e)

    cdmi_info = res.json()
    logger.debug('got CDMI content: {0}'.format(json.dumps(cdmi_info)))
    if not cdmi_info[u'objectType'] == u'application/cdmi-container':
        logger.error("Cannot traverse a file path: {0}".format(path))
        return

    regex_compiled = None
    if include_pattern is not None:
        regex_compiled = re.compile(include_pattern)

    if only_files:
        for f in cdmi_info[u'children']:
            f = f[:-1] if f.endswith('?') else f
            if f.endswith('/'):
                # filter matches with regex
                if include_pattern is None or regex_compiled.match(f) is not None:
                    app.send_task(task_name,
                                  args=[str(path)+f], kwargs={})
    else:
        for o in cdmi_info[u'children']:
            o = o[:-1] if o.endswith('?') else o
            # filter matches with regex
            if include_pattern is None or regex_compiled.match(f) is not None:
                app.send_task(task_name,
                              args=[str(path)+o], kwargs={})

    for x in cdmi_info[u'children']:
        x = x[:-1] if x.endswith('?') else x
        if x.endswith('/'):
            traversal.s(
                str(path)+x, task_name, only_files, include_pattern=include_pattern).apply_async()
