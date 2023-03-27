import os

import pytest

from analytics import utils, LogLevel
from datetime import datetime as dt

SAMPLES_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), 'samples'))


def test_validate_date_string():
    assert utils.validate_date_string('2018-01-01', '%Y-%m') is None
    assert utils.validate_date_string('-2018-01-01', '%Y-%m-%d') is None
    assert utils.validate_date_string('2018-01-01', '%Y-%m-%d') == dt(2018, 1, 1)


def test_is_valid_date_string():
    assert not utils.is_valid_date_string('2018-01-01', '%Y-%m')
    assert utils.is_valid_date_string('2018-01-01', '%Y-%m-%d')
    assert not utils.is_valid_date_string(None, '%Y-%m-%d')


def test_read_lines_from_file():
    # test with dir path
    with pytest.raises(IsADirectoryError):
        next(utils.read_lines_from_file(SAMPLES_PATH))
    # test with wrong file path
    with pytest.raises(FileNotFoundError):
        next(utils.read_lines_from_file(os.path.join(SAMPLES_PATH, 's.txt')))
    # test with correct file path
    assert next(utils.read_lines_from_file(os.path.join(SAMPLES_PATH, 'sample.txt'))) == 'This is a sample file'


def test_is_log_line_type():
    assert not utils.is_page_crawled_log_line('')
    with pytest.raises(ValueError):
        utils.is_page_crawled_log_line(None)
    assert not utils.is_page_crawled_log_line('testing with sample string')
    assert utils.is_page_crawled_log_line('test line %s' % utils.PAGE_CRAWLED_LOG_LINE)
    assert utils.is_page_crawled_log_line('%s test line' % utils.PAGE_CRAWLED_LOG_LINE)
    assert utils.is_page_crawled_log_line('test %s line' % utils.PAGE_CRAWLED_LOG_LINE)
    assert utils.is_page_crawl_error_log_line('test line %s' % utils.PAGE_ERROR_LOG_LINE)
    assert utils.is_page_crawl_error_log_line('%s test line' % utils.PAGE_ERROR_LOG_LINE)
    assert utils.is_page_crawl_error_log_line('test %s line' % utils.PAGE_ERROR_LOG_LINE)
    assert utils.is_log_stats_log_line('test line %s' % utils.LOGS_STATS_LOG_LINE)
    assert utils.is_log_stats_log_line('%s test line' % utils.LOGS_STATS_LOG_LINE)
    assert utils.is_log_stats_log_line('test %s line' % utils.LOGS_STATS_LOG_LINE)


def test_get_log_file_path():
    with pytest.raises(ValueError):
        utils.get_log_file_path(None, None)
    with pytest.raises(ValueError):
        utils.get_log_file_path(None, 1)
    with pytest.raises(ValueError):
        utils.get_log_file_path(None, '')
    with pytest.raises(FileNotFoundError):
        utils.get_log_file_path('2018-01-01', '')
    with pytest.raises(ValueError):
        utils.get_log_file_path('2018-01-01', os.path.join(SAMPLES_PATH, 'logs'), 0)
    assert utils.get_log_file_path('2018-01-01', os.path.join(SAMPLES_PATH, 'logs')) \
           == os.path.join(SAMPLES_PATH, 'logs', 'info.log.2018-01-01')
    assert utils.get_log_file_path('2018-01-01', os.path.join(SAMPLES_PATH, 'logs'), LogLevel.ERROR) \
           == os.path.join(SAMPLES_PATH, 'logs', 'error.log.2018-01-01')
    assert utils.get_log_file_path('2018-01-01', os.path.join(SAMPLES_PATH, 'logs'), LogLevel.INFO) \
           == os.path.join(SAMPLES_PATH, 'logs', 'info.log.2018-01-01')
