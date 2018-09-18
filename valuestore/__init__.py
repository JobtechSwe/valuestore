import logging
import os
from elasticsearch import Elasticsearch

log = logging.getLogger(__name__)

ES_HOST = os.getenv('ES_HOST', 'localhost')
ES_PORT = os.getenv('ES_PORT', 9200)
ES_USER = os.getenv('ES_USER')
ES_PWD = os.getenv('ES_PWD')

log.info("Using Elasticsearch node at %s:%s" % (ES_HOST, ES_PORT))
if ES_USER and ES_PWD:
    elastic = Elasticsearch([{'host': ES_HOST, 'port': ES_PORT,
                              'use_ssl': True, 'sniff_on_start': True,
                              'sniff_on_connection_fail': True, 'sniffer_timeout': 60,
                              'http_auth': "%s:%s" % (ES_USER,
                                                      ES_PWD)}])
else:
    elastic = Elasticsearch([{'host': ES_HOST, 'port': ES_PORT}])
