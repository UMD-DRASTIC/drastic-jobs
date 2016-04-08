from __future__ import absolute_import

from celery import Celery as _Celery
from kombu import Exchange, Queue


class Celery(_Celery):

    def get_message_count(self, queue):
            '''
            Raises: amqp.exceptions.NotFound: if queue does not exist
            '''
            with self.connection_or_acquire() as conn:
                return conn.default_channel.queue_declare(
                    queue=queue, passive=True).message_count

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
                      'routing_key': 'traversal'
                      }}, )
)

if __name__ == '__main__':
    app.start()
