# coding=utf-8
"""Utilities for indexing Indigo objects.

This module does stuff.

"""

from pyld import jsonld
from jsonpath_rw import parse
import yaml
import json
import logging
import os

dirpath = os.path.dirname(os.path.realpath(__file__))

logger = logging.getLogger(__name__)
sh = logging.StreamHandler()
logger.addHandler(sh)
logger.setLevel(logging.INFO)

lindex_config = None
with open(dirpath+'/legacy_index_config.yaml', 'r') as f:
    lindex_config = yaml.load(f)
ldts_fields = lindex_config['dts_fields']
for key, obj in ldts_fields.items():
    if 'jsonpath' in obj:
        obj['expr'] = parse(obj['jsonpath'])


index_config = None
with open(dirpath+'/index_config.yaml', 'r') as f:
    index_config = yaml.load(f)
dts_fields = index_config['dts_fields']
for key in dts_fields.keys():
    dts_fields[key]['expr'] = parse(dts_fields[key]['jsonpath'])
dts_jsonld_frame = None

with open(dirpath+'/dts_jsonld_frame.json', 'r') as f:
    dts_jsonld_frame = json.load(f)


def add_BD_fields(jsonld_str, esdoc):
    expanded = jsonld.expand(json.loads(jsonld_str))
    logger.info("EXPANDED: "+json.dumps(expanded, indent=2))
    framed = jsonld.frame(expanded, dts_jsonld_frame)
    logger.info("FRAMED: "+json.dumps(expanded, indent=2))

    for field, obj in dts_fields.items():
        # logger.info("obj: "+obj)
        esdoc[field] = []
        # append all the matching values to the ES field
        for val in [match.value for match in obj['expr'].find(framed)]:
            esdoc[field].append(val)


def add_BD_fields_legacy(jsonld_str, esdoc):
    legacyjson = json.loads(jsonld_str)
    logger.info("LEGACY: "+json.dumps(legacyjson, indent=2))

    for field, obj in ldts_fields.items():
        # append all the matching values to the ES field
        if 'expr' in obj:
            allvals = [match.value for match in obj['expr'].find(legacyjson)]
            if len(allvals) > 1:
                esdoc[field] = allvals
            elif len(allvals) == 1:
                esdoc[field] = allvals[0]
        elif 'method' in obj:
            method = obj['method']
            allvals = globals()[method](legacyjson)
            if allvals is not None:
                esdoc[field] = allvals


def getContext(context_array, extractor_id):
    for ctx in context_array:
        if 'extractor_id' in ctx['content']:
            if extractor_id == ctx['content']['extractor_id']:
                return ctx
        elif 'extractor_id' in ctx['agent']:
            if extractor_id == ctx['agent']['extractor_id']:
                return ctx
    return None


def caltech101(json):
    threshold = 0
    ctx = getContext(json, 'ncsa.cv.caltech101')
    if ctx is None:
        return
    caltech = []
    for i, score in enumerate(ctx['content']['basic_caltech101_score']):
        if float(score) >= threshold:
            category = ctx['content']['basic_caltech101_category'][i]
            if 'BACKGROUND_Google' == category:
                # ignore 'clutter' category
                continue
            caltech.append({
                'category': category,
                'score': float(score)
                })
    if len(caltech) > 0:
        return caltech


def pixel_count(json):
    ctx = getContext(json,
                     "http://dts-dev.ncsa.illinois.edu:9000" +
                     "/api/extractors/ncsa.image.metadata")
    if ctx is None:
        return
    height = ctx['content']['height']
    width = ctx['content']['width']
    return height * width


if __name__ == '__main__':
    with open(dirpath+'/example_extracts.json', 'r') as f:
        jsonld_str = f.read()
        esdoc = {}
        add_BD_fields(jsonld_str, esdoc)
        logger.info("ESDOC:"+json.dumps(esdoc, indent=2))
    with open(dirpath +
              '/../../samples/dts-legacy-metadata-jsonld.json', 'r') as f:
        jsonld_str = f.read()
        esdoc = {}
        add_BD_fields_legacy(jsonld_str, esdoc)
        logger.info("LEGACY ESDOC:"+json.dumps(esdoc, indent=2))
