"""Explore the data from a class.

Usage:
    import explore
    explore.create('buddyupevents')
    explore.write_event_mapping()
    data = explore.get_data('buddyup-aleck-events-export.json')
    explore.bulk_load_events(data)

TODO:
    - pass in an elasticsearch instance to all the funcs that create a default one or set the connection from
    the envorinoment. e.g. create(es, 'buddyupevents') and bulk_load_events(es, data)
    - set more mappings as 'not_analyzed' meaning the fields won't be tokenized and parsed but still searchable, we
    don't want ids, first names, etc being tokenized, just messages

"""

import json
from elasticsearch import Elasticsearch, NotFoundError
from elasticsearch.helpers import bulk
from elasticsearch_dsl import Mapping, Object
from elasticsearch_dsl.connections import connections
from textblob import TextBlob

# Define a default Elasticsearch connection
connections.create_connection(hosts=['localhost'])


def get_data(filename=None):
    """Load a class's data."""
    if not filename:
        filename = 'buddyup--JzRSuesIM12en8r4FCk-export.json'
    with file(filename, 'r') as f:
        data = json.load(f)
    return data


def create(index='buddyupevents'):
    """Create elastisearch index."""
    es = Elasticsearch()
    es.indices.create(index=index, ignore=400)


def drop(index='buddyupevents'):
    """Drop the Eeasticsearch index."""
    es = Elasticsearch()
    try:
        es.indices.delete(index=index)
    except NotFoundError:
        print("ES index {} missing".format(index))


def bulk_load_feed(data, index='buddyupclass'):
    """Bulk write feed to ES."""
    es = Elasticsearch()

    def serializer(data):
        for k, v in data.iteritems():
            v['_id'] = k
            v['id'] = k
            if v.get('data', {}).get('params') == '':
                v['data']['params'] = None
            if v.get('data', {}).get('password'):
                v['data']['password'] = None
            yield v

    if 'news_feed' in data:
        bulk(
            es,
            serializer(data['news_feed']),
            index=index,
            doc_type='feed',
        )
    else:
        raise KeyError("Expected news_feed in data")


def bulk_load_events(data, index='buddyupevents'):
    """Bulk loads all events."""
    es = Elasticsearch()  # TODO in prod seting config for ES cluster

    def serializer(data):
        for k, v in data.iteritems():
            v['_id'] = k
            v['id'] = k
            if v.get('data', {}).get('params') == '':
                v['data']['params'] = None
            if v.get('data', {}).get('password'):
                v['data']['password'] = None
            if v.get('type') == 'chat_message' and v.get('data', {}).get('body'):
                chat = TextBlob(v.get('data', {}).get('body', ''))
                v['sentiment'] = {
                    'polarity': chat.sentiment.polarity * 100,
                    'subjectivity': chat.sentiment.subjectivity * 100,
                }
            yield v

    bulk(
        es,
        serializer(data),
        index=index,
        doc_type='event',
    )


def write_event_mapping(index='buddyupevents', doc_type='event'):
    """Write the `event` mapping for an ES index and doc type.

    http://elasticsearch-dsl.readthedocs.org/en/latest/persistence.html#mappings
    """
    m = Mapping(doc_type)
    data = Object()  # elasticsearch_dsl field object
    data.field('password', 'string', index='no', include_in_all=False, store=False)
    data.field('first_name', 'string', index='not_analyzed')
    data.field('last_name', 'string', index='not_analyzed')
    data.field('start', 'date')
    data.field('end', 'date')
    m.field('data', data)

    m.field('created_at', 'date')
    m.field('first_name', 'string', index='not_analyzed')
    m.field('last_name', 'string', index='not_analyzed')
    m.field('id', 'string', index='not_analyzed')
    m.field('type', 'string', index='not_analyzed')
    m.save(index)


def view_mappings(index='buddyupevents', doc_type='event'):
    """Return a Mapping of mappings.

    Usage: explore.view_mappings().to_dict()
    """
    m = Mapping.from_es(index, doc_type)
    return m
