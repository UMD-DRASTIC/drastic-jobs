from __future__ import absolute_import
from react.celery import app
import logging


@app.task
def react(operation, path, stateChange):
    """Reacts to the state changes indicated by parameters, queuing up other jobs"""
    logger.info('Job launched for: {0} on {1}'.format(operation, path))
    if "create" == operation:
        index.apply_async((path, stateChange))
        postForExtract.apply_async((path))
    if "update" == operation:
        index.apply_async((path, stateChange))
    if "delete" == operation:
        deindex.apply_async((path))

@app.task
def index(path, stateChanged):
    """Reindexes the metadata for a data object"""
    logger.info('Index job launched for: {0} on {1}'.format(path, stateChanged))
    # TODO GET metadata from Indigo or stageChanged
    # TODO POST fields to Elasticseaerch

@app.task
def deindex(path):
    """Removes a data object from the index"""
    logger.info('Deindex job launched for: {0}'.format(path))
    # TODO Remove item from Elasticseaerch

@app.task
def postForExtract(path):
    """Post a file to the feature extraction service (DTS)"""
    logger.info('Post for extract job launched for: {0}'.format(path))
    # TODO POST file to DTS and parse fileid
    fileid = "foo"
    pollForExtract.apply_async((path, fileid))

@app.task
def pollForExtract(path, fileid):
    """Poll the feature extraction service for the results of an extraction. Re-enqueue this task if still waiting."""
    logger.info('Poll for extract job launched for: {0} with fileid {1}'.format(path, fileid))
    # TODO Check if extract has succeeded or failed
    pollForExtract.apply_async((path, fileid))
    fetchUpdatesFromExtract.apply_async((path, fileid))

@app.task
def fetchUpdatesFromExtract(path, fileid):
    """Fetch the feature extraction results and update repository."""
    logger.info('Fetch updates from extract job launched for: {0} with fileid {1}'.format(path, fileid))
    # TODO GET new metadata
    # TODO POST new metadata in Indigo
