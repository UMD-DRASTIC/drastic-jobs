"""Graph Metadata Model
Used by collections and resources to store user metadata in the graph database.

"""
import logging
import os
from jobs.celery_app import app
from celery.utils.log import get_task_logger
from jobs.util import get_client
import workflow
from gremlin_python.structure.graph import Graph
from gremlin_python.process.graph_traversal import __
from gremlin_python.process.strategies import *
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection

__copyright__ = "Copyright (C) 2016 University of Maryland"
__license__ = "GNU AFFERO GENERAL PUBLIC LICENSE, Version 3"

logger = get_task_logger(__name__)
gremlin_host = os.getenv('GREMLIN_HOST', '127.0.0.1')
gremlin_port = os.getenv('GREMLIN_PORT', 8182)
gremlin_uri = u'ws://{0}:{1}'.format(gremlin_host, gremlin_port)
gremlin_graph = 'drasticgraph.g'


def react(event):
    if event.operation in ["create", "update_object", "update", "update_metadata"]:
        put_graph_metadata.apply_async((event.path,))
    elif event.operation == "delete":
        delete_graph_metadata.apply_async((event.uuid,))


workflow.registry.subscribe(react)


@app.task(bind=True, default_retry_delay=300, max_retries=10)
def put_graph_metadata(self, path):
    """Replaces existing user triples for a single subject."""
    logger.debug(u'PUT RDF metadata for {1}'.format(path))
    path = path[:-1] if path.endswith('?') else path
    is_folder = True if str(path).endswith('/') else False

    try:
        res = get_client().get_cdmi(str(path))
        if res.code() in [404, 403]:
            logger.warn("Dropping task for object that gives a 403/403: {0}".format(path))
            return
        if not res.ok():
            raise IOError("Drastic get_cdmi failed: {0}".format(res.msg()))
        cdmi_info = res.json()
    except IOError as e:
        raise self.retry(exc=e)

    # Drastic fields:
    # FIXME name is not the key, is null
    name = cdmi_info.get('objectName')
    name = name[:-1] if name.endswith('?') else name
    object_UUID = cdmi_info.get('objectID')
    container_UUID = cdmi_info.get('parentID')
    #  parent_URI = cdmi_info.get('parentURI')
    mimetype = 'text/directory'
    if not is_folder:
        mimetype = cdmi_info.get('mimetype')
    metadata = cdmi_info.get('metadata')

    uri = "uuid:{0}".format(object_UUID)
    get_g().V().has('resource', 'URI', uri).drop().count().next()
    t = get_g().addV('resource')
    t = t.property('URI', uri)
    t = t.property('graph', uri)
    t = t.property('name', name)
    t = t.property('mimetype', mimetype)
    for key, value in metadata.iteritems():
        # Don't store metadata without value
        if value is None:  # numeric zero is a valid value
            continue
        t = t.property(key, value)  # key/values as properties
        # TODO add default namespace for keys that are plain tokens
        # t = add_literal_edge(t, uri, key, value)

    # Add contains Edge
    if container_UUID is not None:
        container_uri = "uuid:{0}".format(container_UUID)
        c = get_g().V().has('resource', 'URI', container_uri)
        # TODO fully qualify URIs
        t = t.addE('contains').from_(c)

    t.next()
    logging.debug(u'Created resource vertex for {0}'.format(object_UUID))


def add_literal_edge(traversal, graph_uri, predicate_uri, value):
    traversal = traversal.addE('statement').property('URI', predicate_uri)
    traversal = traversal.to(get_g().addV('literal').property('graph', graph_uri)
                             .property('type', 'xsd:string').property('value', value)).outV()
    return traversal


@app.task(bind=True, default_retry_delay=300, max_retries=10)
def delete_graph_metadata(uuid):
    """Drop graph Vertex for resource and it's properties"""
    count = get_g().V().has('resource', 'drastic:uuid', uuid).drop().count().next()
    logging.debug(u'Dropped graph metadata for {0}, count {1} (should be 0)'
                  .format(uuid, str(count)))


def get_g():
    connection = DriverRemoteConnection(gremlin_uri, gremlin_graph)
    graph = Graph()
    g = graph.traversal().withRemote(connection)
    return g
