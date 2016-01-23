"""Listener for Reacting to Indigo
Listens for Indigo CRUD events over MQTT and creates a delayed celery task to analyze and
react to those events as compute resources allow it. By default this program listens for
changes anywhere in the Indigo hierarchy. However, you may instead supply a list of
paths that you want watched.

Usage:
  listener.py [--host=<Indigo host>] [--port=<MQTT port>] [--quiet | --verbose] [PATH ...]
  listener.py -h | --help

Options:
  -host       Hostname of Indigo server [default: localhost]
  -port       Port of MQTT server [default: 1883]
  -h, --help  Show this message.
  --verbose   Increase logging output to DEBUG level.
  --quiet     Decrease logging output to WARNING level.

"""

import paho.mqtt.client as mqtt
import json
import logging
import signal
import gevent
from workers.celery import app
from workers.tasks import react
from docopt import docopt

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    logger.info('Connected to MQTT broker with result code {0}'.format(rc))
    client.subscribe("#")

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    logger.debug('Got message: {0}'.format(msg.topic))
    parts = msg.topic.split('/')
    operation = parts[0]
    path = '/'.join(parts[3:])

    # TODO See if path includes a watched_paths prefix

    ops = ('delete', 'create', 'update')
    logger.debug('Got payload: {0}'.format(msg.payload))
    payload = json.loads(msg.payload)
    # Queue the react job with message content
    react.apply_async((operation, path, payload))

def on_disconnect(client, userdata, rc):
    if rc != 0:
        logger.warning('Unexpected disconnection from MQTT broker with result code {0}'.format(rc))
    else:
        logger.info('Disconnected from MQTT broker')

def init_mqtt():
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect
    client.connect(mqtt_host, 1883, 60)
    return client

def mqtt_loop():
    while True:
        mqtt_client.loop()
        gevent.sleep(0)


def shutdown(_signo, _stack_frame):
    logger.info('Stopping MQTT...')
    mqtt_client.disconnect()
    logger.info('Dereticulating splines... Done!')
    sys.exit(0)

if __name__ == '__main__':
    signal.signal(signal.SIGTERM, shutdown)
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

    mqtt_host = arguments["--host"]
    mqtt_port = arguments["--port"]
    watched_paths = arguments["PATH"] # An array of paths we don't ignore
    if len(watched_paths) == 0:
        watched_paths = ['/']

    DEVNULL = open('/dev/null', 'w')
    mqtt_client = init_mqtt()
    mqtt_loop()
