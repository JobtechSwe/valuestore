import os, sys, pytest
from valuestore import taxonomy
from valuestore.taxonomy import ES_TAX_INDEX
from elasticsearch import Elasticsearch

ES_HOST = os.getenv("ES_HOST", "localhost")
ES_PORT = os.getenv("ES_PORT", 9200)
#elastic = Elasticsearch([{'host': settings.ES_HOST, 'port': settings.ES_PORT}])
elastic = Elasticsearch([{'host': ES_HOST, 'port': ES_PORT}])

def find(key, dictionary): #about yield: https://stackoverflow.com/questions/231767/what-does-the-yield-keyword-do
    for k, v in dictionary.items():
        if k == key:
            yield v
        elif isinstance(v, dict):
            for result in find(key, v):
                yield result
        elif isinstance(v, list):
            for d in v:
                if isinstance(d, dict):
                    for result in find(key, d):
                        yield result

@pytest.mark.parametrize("query_string", [None, [], '', 'query_1'])
@pytest.mark.parametrize("taxonomy_code", [None, [], '', 'taxkod_1', ['t1', 't2'] ])
@pytest.mark.parametrize("entity_type", [None, [], '', 'entity_1', ['e1','e2']])
@pytest.mark.parametrize("offset, limit", [[0, 10], [0, 1]])
def test_find_concepts(query_string, taxonomy_code, entity_type, offset, limit):
    print('===================', sys._getframe().f_code.co_name, '===================')
    d = taxonomy._build_query(query_string, taxonomy_code,
                              entity_type, offset, limit)
    print(d)
    # print(query_string, taxonomy_code, entity_type, offset, limit)
    if not query_string:  # query_string == None,[]
        assert d['from'] == 0 # alt. assert list(find('from', d)) == [0]
        assert d['size'] == 5000 # alt. assert list(find('size', d)) == [5000]
        assert d['sort'] == [{'num_id': {'order': 'asc'}}] # alt. assert list(find('order', d)) == ['asc']
        if not taxonomy_code and not entity_type:
            assert d['query']['match_all'] == {} # alt. assert list(test_kandidater.find('match_all', d)) == [{}]
    else:  # query_string not empty
        musts = d['query']['bool']['must']
        assert d['from'] == offset
        assert d['size'] == limit
        assert {'term': {'label.autocomplete': query_string}} in musts # alt. assert list(find('label.autocomplete', d)) == [query_string]
        if taxonomy_code:
            assert {'term': {'parent.id': taxonomy_code}} in musts
        if entity_type:
            assert {'term': {'type': entity_type}} in musts
        assert d['from'] == offset
        assert d['size'] == limit

def test_format_response(elastic_response): # see elastic_response fixture in conftest.py
    print('============================',sys._getframe().f_code.co_name,'============================ ')
    d = taxonomy.format_response(elastic_response)
    print(d)
    if elastic_response:
        assert  d['antal'] == elastic_response['hits']['total']
        assert list(find('kod', d)) == list(find('id', elastic_response))
        assert list(find('term', d)) == list(find('label', elastic_response))
        assert list(find('typ', d)) == list(find('type', elastic_response))
    else:
        assert d['antal'] == None and d['entiteter'] == []

def test_get_concept():
    print(get_concept(elastic, 1, 1))

def get_concept(elastic, tax_id, tax_typ):
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
    return taxonomy.format_response(elastic.search(index=ES_TAX_INDEX, body=query_dsl))
