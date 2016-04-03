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
from elasticsearch_dsl import Mapping, Object, Nested
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
    data.field('accepted_by', 'string', index='not_analyzed')
    data.field('accepted_by_last_name', 'string', index='not_analyzed')
    data.field('accepted_by_first_name', 'string', index='not_analyzed')
    data.field('requested_by', 'string', index='not_analyzed')
    data.field('requested_by_last_name', 'string', index='not_analyzed')
    data.field('requested_by_first_name', 'string', index='not_analyzed')
    data.field('recipient', 'string', index='not_analyzed')
    data.field('recipient_first_name', 'string', index='not_analyzed')
    data.field('recipient_last_name', 'string', index='not_analyzed')
    data.field('sender', 'string', index='not_analyzed')
    data.field('sender_first_name', 'string', index='not_analyzed')
    data.field('sender_last_name', 'string', index='not_analyzed')
    data.field('sender_last_name', 'string', index='not_analyzed')
    data.field('sent_at', 'date')
    data.field('start', 'date')
    data.field('end', 'date')
    m.field('data', data)

    m.field('created_at', 'date')
    m.field('first_name', 'string', index='not_analyzed')
    m.field('last_name', 'string', index='not_analyzed')
    m.field('id', 'string', index='not_analyzed')
    m.field('creator', 'string', index='not_analyzed')
    m.field('involved', 'string', index='not_analyzed')
    m.field('type', 'string', index='not_analyzed')
    m.save(index)


def view_mappings(index='buddyupevents', doc_type='event'):
    """Return a Mapping of mappings.

    Usage: explore.view_mappings().to_dict()
    """
    m = Mapping.from_es(index, doc_type)
    return m


def write_user_mapping(index='buddyupusers', doc_type='user'):
    """Write the `user` mapping for buddy up."""
    m = Mapping(doc_type)

    public = Object()
    public.field('first_name', 'string', index='not_analyzed')
    public.field('last_name', 'string', index='not_analyzed')
    public.field('signed_up_at', 'date')
    m.field('public', public)

    groups = Nested()
    groups.field('creator', 'string', index='not_analyzed')
    groups.field('group_id', 'string', index='not_analyzed')
    groups.field('school_id', 'string', index='not_analyzed')
    groups.field('subject', 'string', index='not_analyzed')
    groups.field('subject_code', 'string', index='not_analyzed')
    groups.field('subject_icon', 'string', index='not_analyzed')
    groups.field('start', 'date')
    groups.field('end', 'date')
    m.field('groups', groups)

    private = Object()
    m.field('private', private)

    internal = Object()
    m.field('internal', internal)

    schools = Nested()
    m.field('schools', schools)

    classes = Nested()
    classes.field('course_id', 'string', index='not_analyzed')
    classes.field('id', 'string', index='not_analyzed')
    classes.field('school_id', 'string', index='not_analyzed')
    classes.field('subject_icon', 'string', index='not_analyzed')
    m.field('classes', classes)

    buddies = Nested()
    buddies.field('user_id', 'string', index='not_analyzed')
    buddies.field('first_name', 'string', index='not_analyzed')
    buddies.field('last_name', 'string', index='not_analyzed')
    m.field('buddies', buddies)

    buddies_outgoing = Nested()
    buddies_outgoing.field('user_id', 'string', index='not_analyzed')
    m.field('buddies_outgoing', buddies_outgoing)

    m.save(index)


def create_users_from_events(events, users_index='buddyupusers'):
    """Create the Users index."""
    es = Elasticsearch()

    drop(users_index)
    create(users_index)
    write_user_mapping(users_index)

    def serializer(events):
        for k, v in events.iteritems():
            v['_id'] = k
            v['id'] = k
            v.pop('events', None)
            v.pop('pictures', None)
            v['groups'] = v.get('groups', {}).values()
            v['schools'] = v.get('schools', {}).values()
            v['classes'] = v.get('classes', {}).values()
            v['buddies'] = v.get('buddies', {}).values()
            v['buddies-outgoing'] = [
                {'user_id': key, 'accepted': val}
                for key, val in v.get('buddies-outgoing', {}).iteritems()
            ]
            yield v

    bulk(
        es,
        serializer(events),
        index=users_index,
        doc_type='user',
    )
