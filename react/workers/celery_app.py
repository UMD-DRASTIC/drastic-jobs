from __future__ import absolute_import

from celery import Celery as _Celery


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


app = Celery('react')

# import celery config file
app.config_from_object('workers.celeryconfig')


if __name__ == '__main__':
    app.start()
