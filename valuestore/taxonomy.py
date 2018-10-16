import logging
import os
import json
from elasticsearch.exceptions import RequestError

log = logging.getLogger(__name__)

ES_TAX_INDEX = os.getenv('ES_TAX_INDEX', 'taxonomy')
taxonomy_cache = {}

# Constants
OCCUPATION = 'yrkesroll'
GROUP = 'yrkesgrupp'
FIELD = 'yrkesomrade'
SKILL = 'kompetens'
PLACE = 'plats'
MUNICIPALITY = 'kommun'
REGION = 'lan'
COUNTRY = 'land'
LANGUAGE = 'sprak'
WORKTIME_EXTENT = 'arbetstidsomfattning'
EMPLOYMENT_TYPE = 'anstallningstyp'
DRIVING_LICENCE = 'korkort'

# TODO: Check if taxtype and taxtype_legend can be combined
tax_type = {
    OCCUPATION: 'jobterm',
    GROUP: 'jobgroup',
    FIELD: 'jobfield',
    SKILL: 'skill',
    MUNICIPALITY: 'municipality',
    REGION: 'region',
    COUNTRY: 'country',
    WORKTIME_EXTENT: 'worktime_extent',
    PLACE: 'place',
    LANGUAGE: 'language',
    EMPLOYMENT_TYPE: 'employment_type',
    DRIVING_LICENCE: 'driving_licence'
}
taxtype_legend = {
    'yrke': 'jobterm',
    'yrkesroll': 'jobterm',
    'yrkesgrupp': 'jobgroup',
    'yrkesomrade': 'jobfield',
    'sprak': 'language',
    'kompetens': 'skill',
    'kommun': 'municipality',
    'lan': 'region',
    'sprak': 'language',
    'land': 'country',
    'utbildningsinriktning': 'education',
    'utbildningsniva': 'education_level',
    'korkort': 'driving_licence',
    'varaktighet': 'duration_type',
    'lonetyp': 'wage_type',
    'anstallningstyp': 'employment_type',
    'arbetstidstyp': 'worktime_extent',
}

reverse_tax_type = {item[1]: item[0] for item in tax_type.items()}


def get_concept(elastic_client, tax_id, tax_typ):
    query_dsl = {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"id": tax_id}},
                        {"term": {"type": tax_typ}}
                    ]
                }
            }
        }
    return format_response(elastic_client.search(index=ES_TAX_INDEX, body=query_dsl))


def _build_query(query_string, taxonomy_code, entity_type, offset, limit):
    musts = []
    sort = None
    if query_string:
        musts.append({"term": {"label": query_string}})
    else:
        offset = 0  # TODO parametrise offset and limit
        limit = 5000
        # Sort numerically for non-query_string-queries
        sort = [
            {
                "num_id": {"order": "asc"}
            }
        ]
    if taxonomy_code:
        parent_or_grandparent = {"bool": {"should": [
            {"term": {"parent.id": taxonomy_code}},
            {"term": {"parent.parent.id": taxonomy_code}}
        ]}}
        # musts.append({"term": {"parent.id": taxonomy_code}})
        musts.append(parent_or_grandparent)
    if entity_type:
        musts.append({"term": {"type": entity_type}})

    if not musts:
        query_dsl = {"query": {"match_all": {}}, "from": offset, "size": limit}
    else:
        query_dsl = {
                "query": {
                    "bool": {
                        "must": musts
                    }
                },
                "from": offset,
                "size": limit
            }
    if sort:
        query_dsl['sort'] = sort
    return query_dsl


def get_term(elastic_client, taxtype, taxid):
    if taxtype not in taxonomy_cache:
        taxonomy_cache[taxtype] = {}
    if taxid in taxonomy_cache[taxtype]:
        return taxonomy_cache[taxtype][taxid]
    taxonomy_entity = get_entity(elastic_client, taxtype, taxid, {})
    label = None
    if 'label' in taxonomy_entity:
        label = taxonomy_entity['label']
    if 'term' in taxonomy_entity:
        label = taxonomy_entity['term']
    taxonomy_cache[taxtype][taxid] = label
    return label


def get_entity(elastic_client, taxtype, taxid, not_found_response=None):
    doc_id = "%s-%s" % (taxtype_legend.get(taxtype, ''), taxid)
    taxonomy_entity = elastic_client.get_source(index=ES_TAX_INDEX,
                                                id=doc_id,
                                                doc_type='_all', ignore=404)
    if not taxonomy_entity:
        log.warning("No taxonomy entity found for type %s and id %s" % (taxtype, taxid))
        return not_found_response
    return taxonomy_entity


def find_concepts(elastic_client, query_string=None, taxonomy_code=None, entity_type=None,
                  offset=0, limit=10):
    query_dsl = _build_query(query_string, taxonomy_code, entity_type, offset, limit)
    log.debug("Query: %s" % json.dumps(query_dsl))
    try:
        elastic_response = elastic_client.search(index=ES_TAX_INDEX, body=query_dsl)
        log.debug("Response: %s" % json.dumps(elastic_response))
        return elastic_response
    except RequestError:
        log.error("Failed to query Elasticsearch")
        return None


def format_response(elastic_response):
    response = {
            "antal": elastic_response.get('hits', {}).get('total'),
            "entiteter": []
            }
    for sourcehit in elastic_response.get('hits', {}).get('hits', []):
        hit = sourcehit['_source']
        response['entiteter'].append({"kod": hit['id'], "term": hit['label'],
                                      "typ": hit['type']})

    return response
