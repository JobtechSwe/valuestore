import logging
import os
import json
from elasticsearch.exceptions import RequestError
from . import elastic

log = logging.getLogger(__name__)

ES_TAX_INDEX = os.getenv('ES_TAX_INDEX', 'taxonomy2')

# Constants
OCCUPATION = 'yrkesroll'
GROUP = 'yrkesgrupp'
FIELD = 'yrkesomrade'
SKILL = 'kompetens'
PLACE = 'plats'
MUNICIPALITY = 'kommun'
REGION = 'lan'
WORKTIME_EXTENT = 'arbetstidsomfattning'
LANGUAGE = 'sprak'

tax_type = {
    OCCUPATION: 'jobterm',
    GROUP: 'jobgroup',
    FIELD: 'jobfield',
    SKILL: 'skill',
    MUNICIPALITY: 'municipality',
    REGION: 'region',
    WORKTIME_EXTENT: 'worktime_extent',
    PLACE: 'place',
    LANGUAGE: 'language',
}

reverse_tax_type = {item[1]: item[0] for item in tax_type.items()}


def get_concept(tax_id, tax_typ):
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
    return _format_response(elastic.search(index=ES_TAX_INDEX, body=query_dsl))


def find_concepts(query_string=None, taxonomy_code=None, entity_type=None,
                  offset=0, limit=10):
    musts = []
    sort = None
    if query_string:
        musts.append({"term": {"label.autocomplete": query_string}})
    else:
        offset = 0  # TODO parametrise offset ans limit
        limit = 5000
        # No numerical sorting for autocomplete-query
        sort = [
            {
                "num_id": {"order": "asc"}
            }
        ]
    if taxonomy_code:
        musts.append({"term": {"parent.id": taxonomy_code}})
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
    print("name", __name__)
    print("index", ES_TAX_INDEX)
    log.info(json.dumps(query_dsl))
    if sort:
        query_dsl['sort'] = sort
    try:
        elastic_response = elastic.search(index=ES_TAX_INDEX, body=query_dsl)
        return elastic_response
    except RequestError:
        log.error("Failed to query Elasticsearch")
        return None


def _format_response(elastic_response):
    response = {
            "antal": elastic_response.get('hits', {}).get('total'),
            "entiteter": []
            }
    for sourcehit in elastic_response.get('hits', {}).get('hits', []):
        hit = sourcehit['_source']
        response['entiteter'].append({"kod": hit['id'], "term": hit['label'],
                                      "typ": hit['type']})

    return response
