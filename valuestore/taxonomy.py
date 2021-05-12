import logging
import os
import json
from elasticsearch.exceptions import RequestError

log = logging.getLogger(__name__)

ES_TAX_INDEX_ALIAS = os.getenv('ES_TAX_INDEX_ALIAS', 'taxonomy')
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
DEPRECATED_EDUCATION_LEVEL = 'deprecated_educationlevel'
EDUCATION_FIELD = 'educationfield'
DEPRECATED_EDUCATION_FIELD = 'deprecated_educationfield'
DURATION = 'duration'
OCCUPATION_EXPERIENCE = 'experience'


class JobtechTaxonomy:
    REGION = 'region'
    COUNTRY = 'country'
    DEPRECATED_EDUCATION_LEVEL = 'deprecated-education-level'
    DEPRECATED_EDUCATION_FIELD = 'deprecated-education-field'
    DRIVING_LICENCE = 'driving-license'
    EMPLOYMENT_DURATION = 'employment-duration'
    EMPLOYMENT_TYPE = 'employment-type'
    LANGUAGE = 'language'
    MUNICIPALITY = 'municipality'
    OCCUPATION_GROUP = 'occupation-group'
    OCCUPATION_EXPERIENCE_YEARS = 'occupation-experience-years'
    OCCUPATION_FIELD = 'occupation-field'
    OCCUPATION_NAME = 'occupation-name'
    SKILL = 'skill'
    SUN_EDUCATION_LEVEL_1 = 'sun-education-level-1'
    SUN_EDUCATION_LEVEL_2 = 'sun-education-level-2'
    SUN_EDUCATION_LEVEL_3 = 'sun-education-level-3'
    SUN_EDUCATION_FIELD_1 = 'sun-education-field-1'
    SUN_EDUCATION_FIELD_2 = 'sun-education-field-2'
    SUN_EDUCATION_FIELD_3 = 'sun-education-field-3'
    WAGE_TYPE = 'wage-type'
    WORKTIME_EXTENT = 'worktime-extent'


annons_key_to_jobtech_taxonomy_key = {
    'yrkesroll': JobtechTaxonomy.OCCUPATION_NAME,
    OCCUPATION: JobtechTaxonomy.OCCUPATION_NAME,
    GROUP: JobtechTaxonomy.OCCUPATION_GROUP,
    FIELD: JobtechTaxonomy.OCCUPATION_FIELD,
    OCCUPATION_SV: JobtechTaxonomy.OCCUPATION_NAME,
    GROUP_SV: JobtechTaxonomy.OCCUPATION_GROUP,
    FIELD_SV: JobtechTaxonomy.OCCUPATION_FIELD,
    'anstallningstyp': JobtechTaxonomy.EMPLOYMENT_TYPE,
    'lonetyp': JobtechTaxonomy.WAGE_TYPE,
    'varaktighet': JobtechTaxonomy.EMPLOYMENT_DURATION,
    'arbetstidstyp': JobtechTaxonomy.WORKTIME_EXTENT,
    'kommun': JobtechTaxonomy.MUNICIPALITY,
    'lan': JobtechTaxonomy.REGION,
    'land': JobtechTaxonomy.COUNTRY,
    'korkort': JobtechTaxonomy.DRIVING_LICENCE,
    'kompetens': JobtechTaxonomy.SKILL,
    SKILL: JobtechTaxonomy.SKILL,
    SKILL_SV: JobtechTaxonomy.SKILL,
    'sprak': JobtechTaxonomy.LANGUAGE,
    'deprecated_educationlevel': JobtechTaxonomy.DEPRECATED_EDUCATION_LEVEL,
    'deprecated_educationfield': JobtechTaxonomy.DEPRECATED_EDUCATION_FIELD,
    'sun-education-level-1': JobtechTaxonomy.SUN_EDUCATION_LEVEL_1,
    'sun-education-level-2': JobtechTaxonomy.SUN_EDUCATION_LEVEL_2,
    'sun-education-level-3': JobtechTaxonomy.SUN_EDUCATION_LEVEL_3,
    'sun-education-field-1': JobtechTaxonomy.SUN_EDUCATION_FIELD_1,
    'sun-education-field-2': JobtechTaxonomy.SUN_EDUCATION_FIELD_2,
    'sun-education-field-3': JobtechTaxonomy.SUN_EDUCATION_FIELD_3,
}


def get_term(elastic_client, taxtype, taxid):
    if taxtype not in taxonomy_cache:
        taxonomy_cache[taxtype] = {}
    if taxid in taxonomy_cache[taxtype]:
        return taxonomy_cache[taxtype][taxid]
    taxonomy_entity = find_concept_by_legacy_ams_taxonomy_id(elastic_client, taxtype, taxid, {})
    label = None
    if 'label' in taxonomy_entity:
        label = taxonomy_entity['label']
    if 'term' in taxonomy_entity:
        label = taxonomy_entity['term']
    taxonomy_cache[taxtype][taxid] = label
    return label


def get_entity(elastic_client, taxtype, taxid, not_found_response=None):

    # old version doc_id = "%s-%s" % (taxtype_legend.get(taxtype, ''), taxid)
    # doc_id = taxid  # document id changed from type-id format to just the concept_id
    taxonomy_entity = elastic_client.get_source(index=ES_TAX_INDEX_ALIAS,
                                                id=taxid,
                                                doc_type='_all', ignore=404)
    if not taxonomy_entity:
        log.warning(f"No taxonomy entity found for type: {taxtype}, id: {taxid}")
        return not_found_response
    return taxonomy_entity


def find_concept_by_legacy_ams_taxonomy_id(elastic_client, taxonomy_type,
                                           legacy_ams_taxonomy_id,
                                           not_found_response=None):
    query = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"legacy_ams_taxonomy_id": {"value": legacy_ams_taxonomy_id}}},
                ]
            }
        }
    }
    log.info(f"(find_concept_by_legacy_ams_taxonomy_id) default query: {json.dumps(query)}")
    log.info(f"(find_concept_by_legacy_ams_taxonomy_id) taxtype: {taxonomy_type}, legacy_ams_id: {legacy_ams_taxonomy_id}")
    if isinstance(taxonomy_type, str):
        value = annons_key_to_jobtech_taxonomy_key.get(taxonomy_type, '')
        query['query']['bool']['must'].append({"term": {"type": {"value": value}}})
        log.info(f"Taxonomy type is value. Value: {value}")
    elif isinstance(taxonomy_type, list):
        values = [annons_key_to_jobtech_taxonomy_key.get(t) for t in taxonomy_type]
        log.info(f"Taxonomy type is list. Values: {values}")
        query['query']['bool']['must'].append({"terms": {"type": values}})
    log.info(f"Elastic will search in index: {ES_TAX_INDEX_ALIAS} with query: {json.dumps(query)}")
    try:
        elastic_response = elastic_client.search(index=ES_TAX_INDEX_ALIAS, body=query)
    except RequestError as e:
        log.warning("RequestError", str(e))
        return not_found_response

    hits = elastic_response.get('hits', {}).get('hits', [])
    if not hits:
        log.info(f"No taxonomy entity found for type: {taxonomy_type} and legacy id: {legacy_ams_taxonomy_id}")
        return not_found_response
    source = hits[0]['_source']
    log.info(f"(find_concept_by_legacy_ams_taxonomy_id) returns: {source}")
    return source


def find_legacy_ams_taxonomy_id_by_concept_id(elastic_client, taxonomy_type, concept_id, not_found_response=None):
    query = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"concept_id": {"value": concept_id}}},
                ]
            }
        }
    }
    if isinstance(taxonomy_type, str):
        query['query']['bool']['must'].append({"term": {
            "type": {
                "value": annons_key_to_jobtech_taxonomy_key.get(taxonomy_type, '')
            }
        }})
    elif isinstance(taxonomy_type, list):
        values = [annons_key_to_jobtech_taxonomy_key.get(t) for t in taxonomy_type]
        query['query']['bool']['must'].append({"terms": {"type": values}})
    try:
        elastic_response = elastic_client.search(index=ES_TAX_INDEX_ALIAS, body=query)
    except RequestError as e:
        log.warning("RequestError", str(e))
        return not_found_response

    hits = elastic_response.get('hits', {}).get('hits', [])
    if not hits:
        log.debug("No taxonomy entity found for type %s and "
                  "legacy id %s" % (taxonomy_type,
                                    concept_id))
        return not_found_response
    return hits[0]['_source']


'''
def _build_query(query_string, taxonomy_code, entity_type, offset, limit):
    musts = []
    sort = None
    if query_string:
        musts.append({
            "match_phrase_prefix": {
                "label": {
                    "query": query_string
                }
            }
        })
    else:
        # Sort numerically for non-query_string-queries
        sort = [
            {
                "legacy_ams_taxonomy_num_id": {"order": "asc"}
            }
        ]
    if taxonomy_code:
        if not isinstance(taxonomy_code, list):
            taxonomy_code = [taxonomy_code]
        terms = [{"term": {"parent.legacy_ams_taxonomy_id": t}} for t in taxonomy_code]
        terms += [{"term": {"parent.concept_id.keyword": t}} for t in taxonomy_code]
        terms += [{"term":
                   {"parent.parent.legacy_ams_taxonomy_id": t}
                   } for t in taxonomy_code]
        terms += [{"term":
                   {"parent.parent.concept_id.keyword": t}
                   } for t in taxonomy_code]
        parent_or_grandparent = {"bool": {"should": terms}}
        # musts.append({"term": {"parent.id": taxonomy_code}})
        musts.append(parent_or_grandparent)
    if entity_type:
        musts.append({"bool": {"should": [{"term": {"type": et}} for et in entity_type]}})
        # musts.append({"term": {"type": entity_type}})

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
    log.info("Taxonomy QUERY: %s" % json.dumps(query_dsl))
    return query_dsl


def find_info_by_label_name_and_type(elastic_client, label, type_info, not_found_response=None):
    query = {
        "query": {
            "bool": {
                "must": [
                    {"match": {"label": label}},
                    {"match": {"type": type_info}}
                ]
            }
        }
    }

    try:
        elastic_response = elastic_client.search(index=ES_TAX_INDEX_ALIAS, body=query)
    except RequestError as e:
        log.warning("RequestError", str(e))
        return not_found_response

    hits = elastic_response.get('hits', {}).get('hits', [])
    if not hits:
        log.debug("No taxonomy entity found for  %s and " % (label))
        return not_found_response
    return hits[0]['_source']


def find_info_by_label_name(elastic_client, label, not_found_response=None):
    query = {
        "query": {
            "bool": {
                "must": [
                    {"match": {"label": label}},
                ]
            }
        }
    }

    try:
        elastic_response = elastic_client.search(index=ES_TAX_INDEX_ALIAS, body=query)
    except RequestError as e:
        log.warning("RequestError", str(e))
        return not_found_response

    hits = elastic_response.get('hits', {}).get('hits', [])
    if not hits:
        log.debug("No taxonomy entity found for  %s and " % (label))
        return not_found_response
    return hits[0]['_source']


def find_concepts(elastic_client, query_string=None, taxonomy_code=[], entity_type=[],
                  offset=0, limit=10):
    query_dsl = _build_query(query_string, taxonomy_code, entity_type, offset, limit)
    log.debug("Query: %s" % json.dumps(query_dsl))
    try:
        elastic_response = elastic_client.search(index=ES_TAX_INDEX_ALIAS, body=query_dsl)
        log.debug("Find concepts response metrics: took: %s, timed_out: %s" %
                  (elastic_response.get('took', ''),
                   elastic_response.get('timed_out', '')))
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
        response['entiteter'].append({"kod": hit['legacy_ams_taxonomy_id'],
                                      "term": hit['label'],
                                      "typ": hit['type']})

    return response
'''
