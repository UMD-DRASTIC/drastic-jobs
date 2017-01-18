from __future__ import absolute_import
from kombu import Exchange, Queue
from cassandra import ConsistencyLevel
from cassandra.auth import PlainTextAuthProvider
import os
from workers.nara import *
from workers.httpdir import *
from workers.workflow import *
from workers.browndog import *


# TASK MODULES
include = ['workers']

# ROUTING TASKS
task_routes = ({'workers.workflow.traversal': {'queue': 'traversal'},
                'workers.httpdir.ingest_httpdir': {'queue': 'traversal'},
                'workers.nara.ingest_series': {'queue': 'traversal'},
                'workers.nara.schedule_page': {'queue': 'traversal'}})

# MESSAGE BROKER
amqp_host = os.getenv('AMQP_HOST', 'localhost')
amqp_port = os.getenv('AMQP_PORT', '5672')
amqp_user = os.getenv('AMQP_USER', 'guest')
amqp_password = os.getenv('AMQP_PASSWORD', 'guest')
broker_url = 'amqp://{0}:{1}@{2}:{3}//'.format(amqp_user, amqp_password, amqp_host, amqp_port)

# RESULTS BACKEND
result_backend = 'cassandra'
cassandra_host = os.getenv('CASSANDRA_HOST', 'localhost')
cassandra_servers = [cassandra_host]
cassandra_keyspace = 'celery_tasks'
cassandra_table = 'tasks'
cassandra_read_consistency = 'ONE'
cassandra_write_consistency = 'ONE'
# cassandra_auth_provider = PlainTextAuthProvider
# cassandra_user = os.getenv('CASSANDRA_CELERY_USER', 'celery')
# cassandra_password = os.getenv('CASSANDRA_CELERY_PASSWORD', 'password')
# cassandra_auth_kwargs = {
#    'username': cassandra_user,
#    'password': cassandra_password
# }
# cassandra_entry_ttl = 86400  # seconds
