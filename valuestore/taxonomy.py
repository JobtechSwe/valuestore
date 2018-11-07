import logging
import os
import json
from elasticsearch.exceptions import RequestError

log = logging.getLogger(__name__)

ES_TAX_INDEX = os.getenv('ES_TAX_INDEX', 'taxonomy')
taxonomy_cache = {}

# Swedish Constants (not used)
OCCUPATION_SV = 'yrkesroll'
GROUP_SV = 'yrkesgrupp'
FIELD_SV = 'yrkesomrade'
SKILL_SV = 'kompetens'
PLACE_SV = 'plats'
MUNICIPALITY_SV = 'kommun'
REGION_SV = 'lan'
COUNTRY_SV = 'land'
LANGUAGE_SV = 'sprak'
WORKTIME_EXTENT_SV = 'arbetstidsomfattning'
EMPLOYMENT_TYPE_SV = 'anstallningstyp'
DRIVING_LICENCE_SV = 'korkort'
WAGE_TYPE_SV = 'lonetyp'
EDUCATION_LEVEL_SV = 'utbildningsniva'
EDUCATION_FIELD_SV = 'utbildningsinriktning'
DURATION_SV = 'varaktighet'
OCCUPATION_EXPERIENCE_SV = 'erfarenhetsniva'
# English Constants
OCCUPATION = 'occupation'
GROUP = 'group'
FIELD = 'field'
SKILL = 'skill'
PLACE = 'place'
MUNICIPALITY = 'municipality'
REGION = 'region'
COUNTRY = 'country'
LANGUAGE = 'language'
WORKTIME_EXTENT = 'extent'
EMPLOYMENT_TYPE = 'employmenttype'
DRIVING_LICENCE = 'drivinglicence'
WAGE_TYPE = 'wagetype'
EDUCATION_LEVEL = 'educationlevel'
EDUCATION_FIELD = 'educationfield'
DURATION = 'duration'
OCCUPATION_EXPERIENCE = 'experience'

# TODO: Check if taxtype and taxtype_legend can be combined
tax_type = {
    OCCUPATION: 'jobterm',
    OCCUPATION_SV: 'jobterm',
    'yrkesroll': 'jobterm',
    GROUP: 'jobgroup',
    GROUP_SV: 'jobgroup',
    FIELD: 'jobfield',
    FIELD_SV: 'jobfield',
    SKILL: 'skill',
    SKILL_SV: 'skill',
    MUNICIPALITY: 'municipality',
    MUNICIPALITY_SV: 'municipality',
    REGION: 'region',
    REGION_SV: 'region',
    COUNTRY: 'country',
    COUNTRY_SV: 'country',
    WORKTIME_EXTENT: 'worktime_extent',
    WORKTIME_EXTENT_SV: 'worktime_extent',
    PLACE: 'place',
    PLACE_SV: 'place',
    LANGUAGE: 'language',
    LANGUAGE_SV: 'language',
    EMPLOYMENT_TYPE: 'employment_type',
    EMPLOYMENT_TYPE_SV: 'employment_type',
    DRIVING_LICENCE: 'driving_licence',
    DRIVING_LICENCE_SV: 'driving_licence',
    WAGE_TYPE: 'wage_type',
    WAGE_TYPE_SV: 'wage_type',
    EDUCATION_LEVEL: 'education_level',
    EDUCATION_LEVEL_SV: 'education_level',
    EDUCATION_FIELD: 'education',
    EDUCATION_FIELD_SV: 'education',
    DURATION: 'duration_type',
    DURATION_SV: 'duration_type',
    OCCUPATION_EXPERIENCE: 'occupation_experience',
    OCCUPATION_EXPERIENCE_SV: 'occupation_experience'
}
taxtype_legend = {
    'yrke': 'jobterm',
    'yrkesroll': 'jobterm',
    OCCUPATION: 'jobterm',
    'yrkesgrupp': 'jobgroup',
    GROUP: 'jobgroup',
    'yrkesomrade': 'jobfield',
    FIELD: 'jobfield',
    'sprak': 'language',
    LANGUAGE: 'language',
    'kompetens': 'skill',
    SKILL: 'skill',
    'kommun': 'municipality',
    MUNICIPALITY: 'municipality',
    'lan': 'region',
    REGION: 'region',
    'sprak': 'language',
    LANGUAGE: 'language',
    'land': 'country',
    COUNTRY: 'country',
    'utbildningsinriktning': 'education',
    EDUCATION_FIELD: 'education',
    'utbildningsniva': 'education_level',
    EDUCATION_LEVEL: 'education_level',
    'korkort': 'driving_licence',
    DRIVING_LICENCE: 'driving_licence',
    'varaktighet': 'duration_type',
    DURATION: 'duration_type',
    'lonetyp': 'wage_type',
    WAGE_TYPE: 'wage_type',
    'anstallningstyp': 'employment_type',
    EMPLOYMENT_TYPE: 'employment_type',
    'arbetstidstyp': 'worktime_extent',
    WORKTIME_EXTENT: 'worktime_extent',
    'erfarenhetsniva': 'occupation_experience',
    OCCUPATION_EXPERIENCE: 'occupation_experience',
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
        terms = [{"term": {"parent.id": t}} for t in taxonomy_code]
        terms += [{"term": {"parent.parent.id": t}} for t in taxonomy_code]
        parent_or_grandparent = {"bool": {"should": terms}}
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
        log.warning("No taxonomy entity found for type %s and id %s (%s)" % (taxtype,
                                                                             taxid,
                                                                             doc_id))
        return not_found_response
    return taxonomy_entity


def find_concepts(elastic_client, query_string=None, taxonomy_code=[], entity_type=None,
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
