"""Explore the data from a class.

Usage:
    import explore
    explore.create_es('buddyupevents')
    explore.write_event_mapping()
    data = explore.get_data('buddyup-aleck-events-export.json')
    explore.write_feed(data=data, index='buddyupevents')

"""

import json
import lifter
from elasticsearch import Elasticsearch, NotFoundError
from elasticsearch.helpers import bulk
from elasticsearch_dsl import Mapping
from elasticsearch_dsl.connections import connections

# Define a default Elasticsearch connection
connections.create_connection(hosts=['localhost'])

Group = lifter.models.Model('Group')
NewsEvent = lifter.models.Model('NewsEvent')


def get_data(filename=None):
    """Load a class's data."""
    if not filename:
        filename = 'buddyup--JzRSuesIM12en8r4FCk-export.json'
    with file(filename, 'r') as f:
        data = json.load(f)
    return data


def get_groups(data):
    """Get groups."""
    return Group.load(data['groups'].values())


def get_feed(data):
    """Get events."""
    return NewsEvent.load(data['news_feed'].values())


_data = get_data()
groups = get_groups(_data)
feed = get_feed(_data)

print "feed count", feed.count()
print "group count", groups.count()


def create_es(index='buddyupclass'):
    """Create elastisearch index."""
    es = Elasticsearch()
    es.indices.create(index=index, ignore=400)


def drop(index='buddyupclass'):
    """Drop the Eeasticsearch index."""
    es = Elasticsearch()
    try:
        es.indices.delete(index=index)
    except NotFoundError:
        print "ES index missing"


def write_feed(data=None, index='buddyupclass'):
    """Bulk write feed to ES."""
    if data is None:
        data = _data
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
        bulk(
            es,
            serializer(data),
            index=index,
            doc_type='event',
        )


def write_event_mapping(index='buddyupevents', doc_type='event'):
    """Write the mapping for an ES index and doc type.

    http://elasticsearch-dsl.readthedocs.org/en/latest/persistence.html#mappings
    """
    m = Mapping(doc_type)
    m.field('created_at', 'date')
    m.field('data', 'object')
    m.save(index)
