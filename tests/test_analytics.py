import os
from statistics import StatisticsError

import pytest
import analytics
from analytics import models
from datetime import datetime as dt, datetime

SAMPLES_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), 'samples'))


def test_get_page_crawled_attributes():
    case1 = '2021-03-12 15:49:48 INFO:general_spider:PAGE_CRAWLED: ' \
            'url https://www.travelmath.com/cities/Longfeng,+China took 1351.7420291900635 ms and 34073 bytes'
    assert analytics.get_page_crawled_attributes(case1) == (dt(
        2021, 3, 12, 15, 49,
        48), 'https://www.travelmath.com/cities/Longfeng,+China',
                                                            1351.7420291900635,
                                                            34073)
    with pytest.raises(Exception):
        analytics.get_page_crawled_attributes('')
    no_url_case = '2021-03-12 15:49:48 INFO:general_spider:PAGE_CRAWLED: ' \
                  'url took 1351.7420291900635 ms and 34073 bytes'
    with pytest.raises(Exception):
        analytics.get_page_crawled_attributes(no_url_case)
    integer_page_speed_case = '2021-03-12 15:49:48 INFO:general_spider:PAGE_CRAWLED: ' \
                              'url https://www.travelmath.com/cities/Longfeng,+China took 1 ms and 34073 bytes'
    with pytest.raises(Exception):
        analytics.get_page_crawled_attributes(integer_page_speed_case)


def test_get_crawler_frequency():
    case1 = '2021-03-12 15:49:46 INFO:scrapy.extensions.logstats:' \
            'Crawled 0 pages (at 0 pages/min), scraped 0 items (at 0 items/min)'
    assert analytics.get_crawler_frequency(case1) == 0


def test_get_page_crawl_error_attributes():
    case1 = '2021-03-12 15:50:05 ERROR:general_spider:PAGE_CRAWL_ERROR: HttpError/Ignoring non-200 response on ' \
            'https://www.travelandleisure.com/www.travelandleisure.com/articles/ex-pats-st-john'
    assert len(analytics.get_page_crawl_error_attributes(case1)) == 3


def test_get_info_logs_summary():
    page_crawled_attributes, crawler_frequencies = analytics.get_info_logs_summary(
        os.path.join(SAMPLES_PATH, 'logs', 'info.log.2021-03-13'))
    assert len(page_crawled_attributes) == 296
    assert len(crawler_frequencies) == 1
    with pytest.raises(FileNotFoundError):
        analytics.get_info_logs_summary(
            os.path.join(SAMPLES_PATH, 'logs', 'info.log.2021-03-14'))


def test_get_error_logs_summary():
    page_crawl_error_attributes = analytics.get_error_logs_summary(
        os.path.join(SAMPLES_PATH, 'logs', 'error.log.2021-03-13'))
    assert len(page_crawl_error_attributes) == 1


def test_get_page_items():
    with pytest.raises(ValueError):
        analytics.get_page_items(None)
    assert analytics.get_page_items([]) == []
    with pytest.raises(AttributeError):
        analytics.get_page_items([1])
        analytics.get_page_items([{}])
        analytics.get_page_items([{'url': ''}])
    assert analytics.get_page_items([models.LogItem('', '', 0, 0, '')]) == [
        ('', 1, '', 0.0, 0, '', '', False, '')
    ]
    assert analytics.get_page_items([
        models.LogItem('url1', '2018-01-01', 500, 500, None),
        models.LogItem('url1', '2018-01-01', 600, 500, None)
    ]) == [
        models.PageItem('url1', 2, '', 500.0, 500, '2018-01-01', '2018-01-01',
                        True, None)
    ]
    assert analytics.get_page_items([
        models.LogItem('url1', '2018-01-01', 500, 500, None),
        models.LogItem('url1', '2018-01-02', 600, 500, None)
    ]) == [
        models.PageItem('url1', 2, '', 600.0, 500, '2018-01-01', '2018-01-02',
                        True, None)
    ]


def test_get_domain_items():
    with pytest.raises(ValueError):
        analytics.get_domain_items(None, None)
    with pytest.raises(ValueError):
        analytics.get_domain_items([], None)
    assert analytics.get_domain_items([], datetime(2018, 1, 1)) == []
    # case1: single domain with all compliant
    page_items_case1 = [
        models.PageItem('https://www.example.com/test', 2, 'www.example.com',
                        600.0, 500, '2018-01-01', '2018-01-02', True, None),
        models.PageItem('https://www.example.com/test2', 3, 'www.example.com',
                        500.0, 500, '2018-01-01', '2018-01-02', True, None)
    ]

    date_case1 = datetime(2018, 1, 1)

    expected_result_case1 = [
        models.DomainItem(datetime(2018, 1, 1), 'www.example.com', 2, 5, 550.0,
                          1000, 2, 0, [])
    ]

    assert analytics.get_domain_items(page_items_case1,
                                      date_case1) == expected_result_case1
    # case2: multiple domains with all compliant
    page_items_case2 = [
        models.PageItem('https://www.example.com/test', 2, 'www.example.com',
                        600.0, 500, '2018-01-01', '2018-01-02', True, None),
        models.PageItem('https://www.example.com/test2', 3, 'www.example.com',
                        500.0, 500, '2018-01-01', '2018-01-02', True, None),
        models.PageItem('https://www.sample.com/test', 2, 'www.sample.com',
                        600.0, 500, '2018-01-01', '2018-01-02', True, None),
        models.PageItem('https://www.sample.com/test2', 3, 'www.sample.com',
                        500.0, 500, '2018-01-01', '2018-01-02', True, None)
    ]

    date_case2 = datetime(2018, 1, 1)

    expected_result_case2 = [
        models.DomainItem(datetime(2018, 1, 1), 'www.example.com', 2, 5, 550.0,
                          1000, 2, 0, []),
        models.DomainItem(datetime(2018, 1, 1), 'www.sample.com', 2, 5, 550.0,
                          1000, 2, 0, [])
    ]

    assert analytics.get_domain_items(page_items_case2,
                                      date_case2) == expected_result_case2
    # case3: multiple domains with compliant and non compliant urls
    page_items_case3 = [
        models.PageItem('https://www.example.com/test', 2, 'www.example.com',
                        600.0, 500, '2018-01-01', '2018-01-02', True, None),
        models.PageItem('https://www.example.com/test2', 3, 'www.example.com',
                        500.0, 500, '2018-01-01', '2018-01-02', True, None),
        models.PageItem('https://www.sample.com/test', 2, 'www.sample.com',
                        600.0, 500, '2018-01-01', '2018-01-02', True, None),
        models.PageItem('https://www.sample.com/test2', 3, 'www.sample.com',
                        500.0, 500, '2018-01-01', '2018-01-02', True, None),
        models.PageItem('https://www.sample.com/test3', 3, 'www.sample.com',
                        500.0, 500, '2018-01-01', '2018-01-02', False,
                        'HttpError')
    ]

    date_case3 = datetime(2018, 1, 1)

    expected_result_case3 = [
        models.DomainItem(datetime(2018, 1, 1), 'www.example.com', 2, 5, 550.0,
                          1000, 2, 0, []),
        models.DomainItem(datetime(2018, 1, 1), 'www.sample.com', 3, 8,
                          533.3333333333334, 1500, 2, 1, [{
                              'count': 1,
                              'reason': 'HttpError'
                          }])
    ]

    assert analytics.get_domain_items(page_items_case3,
                                      date_case3) == expected_result_case3


def test_get_overview_item():
    # domain_items, page_items, crawler_frequencies, date
    # test with None parameters
    with pytest.raises(ValueError):
        analytics.get_overview_item(None, None, None, None)
    # test with empty domain list and None all
    with pytest.raises(ValueError):
        analytics.get_overview_item([], None, None, None)
    # test with empty lists and None date
    with pytest.raises(ValueError):
        analytics.get_overview_item([], [], [], None)
    with pytest.raises(StatisticsError):
        analytics.get_overview_item([], [], [], datetime(2018, 1, 1))

    page_items = [
        models.PageItem('https://www.example.com/test', 2, 'www.example.com',
                        600.0, 500, '2018-01-01', '2018-01-02', True, None),
        models.PageItem('https://www.example.com/test2', 3, 'www.example.com',
                        500.0, 500, '2018-01-01', '2018-01-02', True, None),
        models.PageItem('https://www.sample.com/test', 2, 'www.sample.com',
                        600.0, 500, '2018-01-01', '2018-01-02', True, None),
        models.PageItem('https://www.sample.com/test2', 3, 'www.sample.com',
                        500.0, 500, '2018-01-01', '2018-01-02', True, None),
        models.PageItem('https://www.sample.com/test3', 3, 'www.sample.com',
                        500.0, 500, '2018-01-01', '2018-01-02', False,
                        'HttpError')
    ]

    domain_items = [
        models.DomainItem(datetime(2018, 1, 1), 'www.example.com', 2, 5, 550.0,
                          1000, 2, 0, []),
        models.DomainItem(datetime(2018, 1, 1), 'www.sample.com', 3, 8,
                          533.3333333333334, 1500, 2, 1, [{
                              'count': 1,
                              'reason': 'HttpError'
                          }])
    ]

    crawler_frequencies = [200, 300]

    date = datetime(2018, 1, 1)

    expected_overview_item = {
        'date': datetime(2018, 1, 1, 0, 0),
        'page_count': 5,
        'visit_count': 13,
        'urls_per_domain_mean': 2.5,
        'total_page_size': 2500,
        'compliance_count': 4,
        'non_compliance_count': 1,
        'crawl_frequency': 250,
        'avg_page_load_speed': 540.0,
        'page_load_speed_count': {
            'fast': 0,
            'medium': 5,
            'slow': 0
        },
        'non_compliance_reasons_count': [{
            'reason': 'HttpError',
            'count': 1
        }]
    }

    assert analytics.get_overview_item(domain_items, page_items,
                                       crawler_frequencies,
                                       date) == expected_overview_item
