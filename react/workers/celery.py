from __future__ import absolute_import

from celery import Celery as _Celery
from kombu import Exchange, Queue


class Celery(_Celery):

    def get_message_count(self, queue='default'):
            '''
            Raises: amqp.exceptions.NotFound: if queue does not exist
            '''
            with self.connection_or_acquire() as conn:
                return conn.default_channel.queue_declare(
                    queue=queue, passive=True).message_count

    def check_traversal_okay(self, task):
        # reschedule this traverse if default queue is already large
        traversal_count = app.get_message_count(queue='traversal')
        default_count = app.get_message_count()
        if(default_count > 5000):
            exc = DelayedTraverseError("Delaying Traverse (default: {0}, traversal: {1})"
                                       .format(default_count, traversal_count))
            raise task.retry(queue="traversal", exc=exc, countdown=300, max_retries=100)


class DelayedTraverseError(Exception):
    pass

    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


app = Celery('react',
             broker='amqp://',
             backend='amqp://',
             include=['workers.tasks'])

# Optional configuration, see the application user guide.
app.conf.update(
    CELERY_TASK_RESULT_EXPIRES=3600,
    CELERY_QUEUES=(
        Queue('default', Exchange('default'), routing_key='default'),
        Queue('traversal', Exchange('traversal'), routing_key='traversal'),
        ),
    CELERY_DEFAULT_QUEUE='default',
    CELERY_DEFAULT_EXCHANGE_TYPE='direct',
    CELERY_DEFAULT_ROUTING_KEY='default',
    CELERY_ROUTES=({'workers.tasks.traversal': {
                      'queue': 'traversal',
                      'routing_key': 'traversal'},
                    'workers.tasks.ingest_httpdir': {
                      'queue': 'traversal',
                      'routing_key': 'traversal'}
                    })
)

if __name__ == '__main__':
    app.start()
