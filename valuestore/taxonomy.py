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
COLLECTION_SV = 'yrkessamlingar'
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
OCCUPATION = 'occupation-name'
GROUP = 'occupation-group'
FIELD = 'occupation-field'
COLLECTION = 'occupation-collection'
SKILL = 'skill'
PLACE = 'place'
MUNICIPALITY = 'municipality'
REGION = 'region'
COUNTRY = 'country'
LANGUAGE = 'language'
WORKTIME_EXTENT = 'worktime-extent'
EMPLOYMENT_TYPE = 'employment-type'
DRIVING_LICENCE = 'driving-license'
DRIVING_LICENCE_REQUIRED = 'driving-license-required'
WAGE_TYPE = 'wage-type'
EDUCATION_LEVEL = 'educationlevel'
DEPRECATED_EDUCATION_LEVEL = 'deprecated_educationlevel'
EDUCATION_FIELD = 'educationfield'
DEPRECATED_EDUCATION_FIELD = 'deprecated_educationfield'
DURATION = 'employment-duration'
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

tax_type = {
    OCCUPATION: JobtechTaxonomy.OCCUPATION_NAME,
    OCCUPATION_SV: JobtechTaxonomy.OCCUPATION_NAME,
    'yrkesroll': JobtechTaxonomy.OCCUPATION_NAME,
    GROUP: JobtechTaxonomy.OCCUPATION_GROUP,
    GROUP_SV: JobtechTaxonomy.OCCUPATION_GROUP,
    FIELD: JobtechTaxonomy.OCCUPATION_FIELD,
    FIELD_SV: JobtechTaxonomy.OCCUPATION_FIELD,
    SKILL: JobtechTaxonomy.SKILL,
    SKILL_SV: JobtechTaxonomy.SKILL,
    MUNICIPALITY: JobtechTaxonomy.MUNICIPALITY,
    MUNICIPALITY_SV: JobtechTaxonomy.MUNICIPALITY,
    REGION: JobtechTaxonomy.REGION,
    REGION_SV: JobtechTaxonomy.REGION,
    COUNTRY: JobtechTaxonomy.COUNTRY,
    COUNTRY_SV: JobtechTaxonomy.COUNTRY,
    WORKTIME_EXTENT: JobtechTaxonomy.WORKTIME_EXTENT,
    WORKTIME_EXTENT_SV: JobtechTaxonomy.WORKTIME_EXTENT,
    PLACE: 'place',
    PLACE_SV: 'place',
    LANGUAGE: JobtechTaxonomy.LANGUAGE,
    LANGUAGE_SV: JobtechTaxonomy.LANGUAGE,
    EMPLOYMENT_TYPE: JobtechTaxonomy.EMPLOYMENT_TYPE,
    EMPLOYMENT_TYPE_SV: JobtechTaxonomy.EMPLOYMENT_TYPE,
    DRIVING_LICENCE: JobtechTaxonomy.DRIVING_LICENCE,
    DRIVING_LICENCE_SV: JobtechTaxonomy.DRIVING_LICENCE,
    WAGE_TYPE: JobtechTaxonomy.WAGE_TYPE,
    WAGE_TYPE_SV: JobtechTaxonomy.WAGE_TYPE,
    EDUCATION_LEVEL: JobtechTaxonomy.DEPRECATED_EDUCATION_LEVEL,
    EDUCATION_LEVEL_SV: JobtechTaxonomy.DEPRECATED_EDUCATION_LEVEL,
    EDUCATION_FIELD: JobtechTaxonomy.DEPRECATED_EDUCATION_FIELD,
    EDUCATION_FIELD_SV: JobtechTaxonomy.DEPRECATED_EDUCATION_FIELD,
    DURATION: JobtechTaxonomy.EMPLOYMENT_DURATION,
    DURATION_SV: JobtechTaxonomy.EMPLOYMENT_DURATION,
    OCCUPATION_EXPERIENCE: JobtechTaxonomy.OCCUPATION_EXPERIENCE_YEARS,
    OCCUPATION_EXPERIENCE_SV: JobtechTaxonomy.OCCUPATION_EXPERIENCE_YEARS,
}

reverse_tax_type = {item[1]: item[0] for item in tax_type.items()}
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
    REGION: JobtechTaxonomy.REGION,
    COUNTRY: JobtechTaxonomy.COUNTRY,
    MUNICIPALITY: JobtechTaxonomy.MUNICIPALITY,
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

