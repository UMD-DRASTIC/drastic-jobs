#!/usr/bin/python
"""Listener for Reacting to Drastic Listens for Drastic CRUD events over MQTT and
creates a delayed celery task to analyze and react to those events as compute
resources allow it. By default this program listens for changes anywhere in the
Drastic hierarchy. However, you may instead supply a list of paths that you want
watched.

Usage:
  listener.py [--host=<Drastic host>] [--port=<MQTT port>] [--quiet | --verbose] [--logfile=<file>]
    [PATH ...]

  listener.py -h | --help

Options:
  -host       Hostname of Drastic server [default: localhost]
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
import sys
from workers.tasks import react
from docopt import docopt


def on_connect(client, userdata, flags, rc):
    """The callback for when the client receives a CONNACK
    response from the server."""
    logger.info('Connected to MQTT broker with result code {0}'.format(rc))
    client.subscribe("#")


def on_message(client, userdata, msg):
    """The callback for when a PUBLISH message is received from the server."""
    logger.debug('Got message: {0}'.format(msg.topic))
    parts = msg.topic.split('/')
    operation = parts[0]
    object_type = parts[1]
    path = '/'.join(parts[2:])
    logger.debug('Got payload: {0}'.format(msg.payload))
    payload = json.loads(msg.payload)
    # Queue the react job with message content
    react.apply_async((operation, object_type, path, payload))


def on_disconnect(client, userdata, rc):
    if rc != 0:
        logger.warning(
            'Unexpected disconnection from MQTT broker with result code {0}'
            .format(rc))
    else:
        logger.info('Disconnected from MQTT broker')


def init_mqtt(mqtt_host):
    logger.info('Connecting to MQTT on '+mqtt_host)
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
    logger.info('SIGTERM detected, stopping MQTT...')
    mqtt_client.disconnect()
    logger.info('Exiting.')
    sys.exit(0)


if __name__ == '__main__':
    signal.signal(signal.SIGTERM, shutdown)
    arguments = docopt(__doc__, version='Listener v1.0')

    logger = logging.getLogger("listener")
    logfile = "listener.log"
    if arguments["--logfile"]:
        logfile = arguments["--logfile"]
    fh = logging.FileHandler(logfile)
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    if arguments['--verbose']:
        logger.setLevel(logging.DEBUG)
    elif arguments['--quiet']:
        logger.setLevel(logging.WARNING)
    else:
        logger.setLevel(logging.INFO)

    mqtt_host = arguments["--host"]
    mqtt_port = arguments["--port"]
    watched_paths = arguments["PATH"]  # An array of paths we don't ignore
    if len(watched_paths) == 0:
        watched_paths = ['/']

    DEVNULL = open('/dev/null', 'w')
    mqtt_client = init_mqtt(mqtt_host)
    logger.info('Entering MQTT loop..')
    mqtt_loop()
