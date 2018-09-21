import pytest


def pytest_generate_tests(metafunc):
    if 'elastic_response' in metafunc.fixturenames:
        metafunc.parametrize("elastic_response", [{"hits": { # value #1
            "total": 657,
            "max_score": 1.0,
            "hits": [
                {
                    "_index": "platsannons",
                    "_type": "doc",
                    "_id": "1",
                    "_score": 1.0,
                    "_source": {
                        "id": 1,
                        "label": "label_1",
                        "type": "type_1",
                        "arbetsplats_id": "82723845",
                    }
                },
                {
                    "_index": "platsannons",
                    "_type": "doc",
                    "_id": "2",
                    "_score": 1.0,
                    "_source": {
                        "id": 2,
                        "label": "label_2",
                        "type": "type_2",
                        "arbetsplats_id": "82723845",
                    }
                }
            ]}},
            {} # value #2
        ], indirect=True)


@pytest.fixture
def elastic_response(request):
    return request.param
