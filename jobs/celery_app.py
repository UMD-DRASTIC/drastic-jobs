
from celery import Celery as _Celery
import os


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
        # traversal_count = app.get_message_count(queue='traversal')
        default_count = app.get_message_count()
        if(default_count > 5000):
            exc = DelayedTraverseError("Delaying Traverse ({0} tasks queued)"
                                       .format(default_count))
            raise task.retry(exc=exc, countdown=300, max_retries=100)


class DelayedTraverseError(Exception):
    pass

    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


app = Celery('react')

# import celery config file
# app.config_from_object('jobs.celeryconfig')

# TASK MODULES
app.conf.update(
    imports=['jobs.workflow', 'jobs.httpdir', 'jobs.browndog', 'jobs.nara'],
    task_default_queue='default',
    task_routes=({'jobs.workflow.traversal': {'queue': 'traversal'},
                  'jobs.httpdir.ingest_httpdir': {'queue': 'traversal'},
                  'jobs.nara.ingest_series': {'queue': 'traversal'},
                  'jobs.nara.schedule_page': {'queue': 'traversal'},
                  'jobs.httpdir.record_batch_count': {'queue': 'notify'},
                  'jobs.httpdir.folders_complete': {'queue': 'notify'},
                  'jobs.httpdir.incr_batch_progress': {'queue': 'notify'}}),
    broker_url='amqp://{0}:{1}@{2}:{3}//'.format(
        os.getenv('AMQP_USER', 'guest'),
        os.getenv('AMQP_PASSWORD', 'guest'),
        os.getenv('AMQP_HOST', 'localhost'),
        os.getenv('AMQP_PORT', '5672')),
    result_backend='cassandra',
    cassandra_servers=[os.getenv('CASSANDRA_HOST', 'localhost')],
    cassandra_keyspace='celery_tasks',
    cassandra_table='tasks',
    cassandra_read_consistency='LOCAL_QUORUM',
    cassandra_write_consistency='LOCAL_QUORUM',
    cassandra_entry_ttl=86400 * 7)  # in seconds (86400 is 24 hours)


if __name__ == '__main__':
    app.start()
