import os
from datetime import datetime
from statistics import mean
import logging

from analytics.utils import is_production_environment, DATE_FORMAT
from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo import UpdateOne

logger = logging.getLogger('db')
DATABASE = 'cygnus_bot_analytics'
CA_BUNDLE_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), os.pardir,
                 'rds-combined-ca-bundle.pem'))

RE_DATABASE = 'test'
RE_COLLECTION = "data"

# collection names
CRAWLED_PAGES = 'crawled_pages'
CRAWLED_DOMAINS = 'crawled_domains'
OVERVIEW = 'overview'
ADVERTISER_DASHBOARD_STATS = 'advertiser_dashboard_stats'

TAXONOMY_COUNT = "taxonomy_count"
INTENT_COUNT = "intent_count"
DOMAINS_DATA = "domains_data"

BID_STREAM = "bidstream"
BID_STREAM_DATEWISE = "bidstream_datewise"

ALL_COLLECTIONS = [CRAWLED_DOMAINS, CRAWLED_PAGES, OVERVIEW,
                   ADVERTISER_DASHBOARD_STATS, TAXONOMY_COUNT, INTENT_COUNT,
                   BID_STREAM, BID_STREAM_DATEWISE, RE_COLLECTION]


def _get_production_uri(creds):
    return "mongodb://%s:%s@%s:27017/?ssl=true&ssl_ca_certs=%s&retryWrites=false" % creds


def _get_local_uri():
    return "mongodb://localhost:27017/"


def _get_mongo_uri(r_engine=None):
    creds = (os.getenv('MONGO_USER'), os.getenv('MONGO_PASSWORD'),
        os.getenv('MONGO_HOST'), CA_BUNDLE_PATH)
    
    if r_engine:
        creds = (os.getenv('RE_MONGO_USER'), os.getenv('RE_MONGO_PASSWORD'),
            os.getenv('RE_MONGO_HOST'), CA_BUNDLE_PATH)

    return _get_production_uri(creds) if is_production_environment(
    ) else _get_local_uri()


_m_client = MongoClient(_get_mongo_uri())
re_m_client = MongoClient(_get_mongo_uri(r_engine=True))


def _setup_db():
    logger.info("setting up database")
    _m_client[DATABASE][CRAWLED_PAGES].create_index([
        ('url', ASCENDING), ('domain', ASCENDING)
    ], unique=True)
    _m_client[DATABASE][CRAWLED_DOMAINS].create_index([
        ('domain', ASCENDING), ('date', DESCENDING)
    ], unique=True)
    _m_client[DATABASE][OVERVIEW].create_index([
        ('date', DESCENDING)
    ], unique=True)
    _m_client[DATABASE][ADVERTISER_DASHBOARD_STATS].create_index([
        ('date', DESCENDING)
    ], unique=True)
    _m_client[DATABASE][TAXONOMY_COUNT].create_index([
        ('date', DESCENDING)
    ], unique=True)
    _m_client[DATABASE][INTENT_COUNT].create_index([
        ('date', DESCENDING)
    ], unique=True)
    
    re_m_client[RE_DATABASE][RE_COLLECTION].create_index('lang')
    re_m_client[RE_DATABASE][BID_STREAM_DATEWISE].create_index([
        ('ingested_on', DESCENDING), 
        ('domain', ASCENDING), 
        ('geo', ASCENDING)
    ], unique=True)
    re_m_client[RE_DATABASE][BID_STREAM].create_index([
        ('domain', ASCENDING)
    ], unique=True)

    logger.info("database setup completed")


_setup_db()


def _is_valid_collection_name(collection_name):
    return collection_name in ALL_COLLECTIONS


def _insert_one(collection_name, document):
    if not _is_valid_collection_name(collection_name):
        raise ValueError('collection value should be one of %s' %
                         ALL_COLLECTIONS)
    try:
        return _m_client[DATABASE][collection_name].insert_one(document)
    except Exception as e:
        print(_insert_one.__name__, e)


def _insert_many(collection_name, documents):
    if not _is_valid_collection_name(collection_name):
        raise ValueError('collection value should be one of %s' %
                         ALL_COLLECTIONS)
    try:
        return _m_client[DATABASE][collection_name].insert_many(documents)
    except Exception as e:
        print(_insert_many.__name__, e)


def _bulk_update(collection_name, update_requests, db_name=DATABASE):
    if not _is_valid_collection_name(collection_name):
        raise ValueError('collection value should be one of %s' %
                         ALL_COLLECTIONS)
    try:
        _client = _m_client if db_name == DATABASE else re_m_client
        return _client[db_name][collection_name].bulk_write(update_requests,
                                                               ordered=False)
    except Exception as e:
        print(_bulk_update.__name__, e)


def create_overview_document(document):
    return _insert_one(OVERVIEW, document)


def create_advertiser_dashboard_stats_item(document):
    return _insert_one(ADVERTISER_DASHBOARD_STATS, document)


def update_overview_document(old_doc, document):
    return _bulk_update(OVERVIEW, [UpdateOne({
        'date': document['date']
    }, {
        '$inc': {
            'page_count': document['page_count'],
            'visit_count': document['visit_count'],
            'total_page_size': document['total_page_size'],
            'compliance_count': document['compliance_count'],
            'non_compliance_count': document['non_compliance_count'],
            'page_load_speed_count.fast': document[
                'page_load_speed_count'].get('fast', 0),
            'page_load_speed_count.medium': document[
                'page_load_speed_count'].get('medium', 0),
            'page_load_speed_count.slow': document[
                'page_load_speed_count'].get('slow', 0)
        },
        '$max': {
            'crawl_frequency': document['crawl_frequency']
        },
        '$set': {
            'avg_page_load_speed': mean([document['avg_page_load_speed'],
                                         old_doc['avg_page_load_speed']]),
            'urls_per_domain_mean': mean([document['urls_per_domain_mean'],
                                          old_doc['urls_per_domain_mean']]),
        },
        '$push': {
            'non_compliance_reasons_count': {
                '$each': document['non_compliance_reasons_count']
            }
        }
    })])


def get_overview_doc_from_db(_date):
    logger.debug('getting overview document')
    if isinstance(_date, datetime):
        return _m_client[DATABASE][OVERVIEW].find_one({'date': _date})
    logger.debug('failed to get overview document due to invalid date')


def create_or_update_overview_document(document):
    result_doc = _m_client[DATABASE][OVERVIEW].find_one(
        {'date': document['date']})
    if result_doc:
        return update_overview_document(result_doc, document)
    return create_overview_document(document)


def create_domain_documents(documents):
    return _insert_many(CRAWLED_DOMAINS, documents)


def create_or_update_domains(documents):
    update_requests = []
    for document in documents:
        update_requests.append(UpdateOne({
            'date': document['date'],
            'domain': document['domain']
        }, {
            '$inc': {
                'page_count': document['page_count'],
                'total_page_size': document['total_page_size'],
                'visit_count': document['visit_count'],
                'compliance_count': document['compliance_count'],
                'non_compliance_count': document['non_compliance_count'],
            },
            '$set': {
                'avg_page_load_speed': document['avg_page_load_speed'],
            },
            '$push': {
                'non_compliance_reasons': {
                    '$each': document['non_compliance_reasons']
                }
            }
        }, upsert=True))
    return _bulk_update(CRAWLED_DOMAINS, update_requests)


def create_or_update_pages_documents(documents):
    update_requests = []
    for document in documents:
        update_requests.append(
            UpdateOne({
                'url': document.url,
                'domain': document.domain
            }, {
                '$inc': {
                    'visit_count': document.visit_count
                },
                '$setOnInsert': {
                    'first_crawled_at': document.first_crawled_at
                },
                '$set': {
                    'last_crawled_at': document.last_crawled_at,
                    'compliant': document.compliant,
                    'page_load_speed': document.page_load_speed,
                    'page_size': document.page_size,
                    'non_compliance_reason': document.non_compliance_reason
                }
            },
                upsert=True))
    return _bulk_update(CRAWLED_PAGES, update_requests)


def create_or_update_count_document(collection_name, document):

    return _m_client[DATABASE][collection_name].update_one(
        { "date": document["date"] }, 
        { "$set": document },
        upsert=True
    )

create_or_update_taxonomy_count_document = lambda document: create_or_update_count_document(TAXONOMY_COUNT, document)
create_or_update_intent_count_document = lambda document: create_or_update_count_document(INTENT_COUNT, document)
create_or_update_urls_count_document = lambda document: create_or_update_count_document(DOMAINS_DATA, document)


def create_or_update_bidstream_records(records):

    return _bulk_update(BID_STREAM_DATEWISE, 
        [
            UpdateOne(
                dict(zip(["ingested_on", "domain", "geo"], key.split("|"))),
                { '$set': values },
                upsert=True
            ) for key, values in records.items()
        ],
        RE_DATABASE
    )

def aggregate_bidstream_records(aggregate_query):

    return re_m_client[RE_DATABASE][BID_STREAM_DATEWISE].aggregate(aggregate_query, allowDiskUse=True)