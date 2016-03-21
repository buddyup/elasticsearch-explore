"""Explore the data from a class."""

import json
import lifter
from elasticsearch import Elasticsearch, NotFoundError
from elasticsearch.helpers import bulk


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


def create_es():
    """Create elastisearch index."""
    es = Elasticsearch()
    es.indices.create(index='buddyupclass', ignore=400)


def drop():
    """Drop the Eeasticsearch index."""
    es = Elasticsearch()
    try:
        es.indices.delete(index='buddyupclass')
    except NotFoundError:
        print "ES index missing"


def write_feed(data=None):
    """Bulk write feed to ES."""
    if data is None:
        data = _data
    es = Elasticsearch()

    def serializer(data):
        for k, v in data.iteritems():
            v['_id'] = k
            v['id'] = k
            yield v
    bulk(
        es,
        serializer(data['news_feed']),
        index='buddyupclass',
        doc_type='feed',
    )
