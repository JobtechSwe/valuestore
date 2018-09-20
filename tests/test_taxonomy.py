import sys
import pytest
from valuestore import taxonomy


@pytest.mark.parametrize("query_string", [None, [], "query_1"])
@pytest.mark.parametrize("taxonomy_code", [None, [], "taxkod_1"])
@pytest.mark.parametrize("entity_type", [None, [], "entity_1"])
@pytest.mark.parametrize("offset, limit", [[0, 10], [0, 1]])
def test_find_concepts(query_string, taxonomy_code, entity_type, offset, limit):
    print('===================', sys._getframe().f_code.co_name, '===================')
    d = taxonomy._build_query(query_string, taxonomy_code,
                              entity_type, offset, limit)
    print(d)
    # print(query_string, taxonomy_code, entity_type, offset, limit)
    if not query_string:  # query_string == None,[]
        assert d['from'] == 0
        assert d['size'] == 5000
        assert d['sort'] == [{'num_id': {'order': 'asc'}}]
        if not taxonomy_code and not entity_type:
            assert d['query']['match_all'] == {}
    else:  # query_string not empty
        musts = d['query']['bool']['must']
        assert {'term': {'label.autocomplete': query_string}} in musts
        # assert ['term']['label.autocomplete'] == query_string
        assert d['from'] == offset
        assert d['size'] == limit
        if taxonomy_code:
            assert {'term': {'parent.id': taxonomy_code}} in musts
        if entity_type:
            assert {'term': {'type': entity_type}} in musts
