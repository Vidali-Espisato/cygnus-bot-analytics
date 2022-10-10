# validate date string
import json
import os
import re
from datetime import datetime

from bson import json_util

RECOMMENDATION_LOG_LINE_GROUP_LENGTH = PAGE_CRAWLED_LOG_LINE_GROUP_LENGTH = 5
CRAWLER_FREQUENCY_LOG_LINE_GROUP_LENGTH = 5
PAGE_CRAWL_ERROR_LOG_LINE_GROUP_LENGTH = 3
PAGE_CRAWL_ERROR_RE_PATTERN = re.compile(
    '^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) ERROR:default:PAGE_CRAWL_ERROR: (.*) on (.*)$')
PAGE_CRAWLED_RE_PATTERN = re.compile(
    r'^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) INFO:default:PAGE_CRAWLED: url (.*) took (\d+\.\d+) ms and (\d+) bytes$')
CRAWLER_FREQUENCY_RE_PATTERN = re.compile(
    r'^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) INFO:scrapy.extensions.logstats:Crawled (\d+) pages \(at (\d+) pages\/min\),' \
    r' scraped (\d+) items \(at (\d+) items\/min\)$')
RECOMMENDATION_ENGINE_RE_PATTERN = re.compile(
    '^(.*) (\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),\d+ \[INFO\] recommendation_engine top1: (.*) top10: (.*) top50: (.*)$'
)

ERROR_LOG_FILENAME = 'error.log.%s'
INFO_LOG_FILENAME = 'info.log.%s'
DATE_FORMAT = '%Y-%m-%d'


class LogLevel:
    INFO = 1
    ERROR = 2


LogLevelFileMapper = {
    LogLevel.INFO: INFO_LOG_FILENAME,
    LogLevel.ERROR: ERROR_LOG_FILENAME
}


def validate_date_string(date_string, date_format):
    try:
        return datetime.strptime(date_string, date_format)
    except Exception:
        return


# check if date_string matches given date_format
def is_valid_date_string(date_string, date_format):
    return bool(validate_date_string(date_string, date_format))


# read file generator
def read_lines_from_file(file_path):
    if not os.path.exists(file_path):
        raise FileNotFoundError(
            'File you\'re trying to read at %s does not exist' % file_path)
    with open(file_path) as fp:
        for line in fp:
            yield line
        fp.close()


# check if it's any log line type
def _is_log_line_type(line, re_pattern):
    if not isinstance(line, str):
        raise ValueError('line should be of str type')
    return bool(re_pattern.match(line))


# check if page crawled log line
def is_page_crawled_log_line(line):
    return _is_log_line_type(line, PAGE_CRAWLED_RE_PATTERN)


# check if log stats log line
def is_log_stats_log_line(line):
    return _is_log_line_type(line, CRAWLER_FREQUENCY_RE_PATTERN)


# check if page crawl error log line:
def is_page_crawl_error_log_line(line):
    return _is_log_line_type(line, PAGE_CRAWL_ERROR_RE_PATTERN)


# check if recommendation engine log line
def is_recommendation_engine_log_line(line):
    return _is_log_line_type(line, RECOMMENDATION_ENGINE_RE_PATTERN)


# dump json file
def write_json_to_file(file_path, file_name, content):
    if not os.path.exists(file_path):
        os.makedirs(file_path)
    with open(os.path.join(file_path, file_name), 'w+') as fp:
        json.dump(content, fp, default=json_util.default)
        fp.close()


# get log file path for error and info
def get_log_file_path(date_string, logs_path, log_level=LogLevel.INFO):
    if not isinstance(logs_path, str):
        raise ValueError('logs_path should be of str type')
    if not is_valid_date_string(date_string, DATE_FORMAT):
        raise ValueError('Invalid date format, format should be %s' %
                         DATE_FORMAT)
    if log_level not in LogLevelFileMapper:
        raise ValueError('file name not mapped for this log level')
    log_file_path = os.path.join(
        logs_path,
        LogLevelFileMapper.get(log_level) % date_string)
    if not os.path.exists(log_file_path):
        raise FileNotFoundError('Log file not exist at %s' % log_file_path)
    return log_file_path


def get_re_match_group(line, re_string, expected_group_length):
    if not isinstance(line, str):
        raise ValueError('line should be of str type')
    groups_list = re_string.findall(line)
    if not groups_list:
        raise Exception('Line is not in specified format expected %s' %
                        re_string)
    group = groups_list[0]
    if len(group) != expected_group_length:
        raise Exception(
            'Line is not in specified format expected %s received %s' %
            (expected_group_length, len(group)))
    return group


def is_production_environment():
    return os.getenv('BUILD_ENV') == 'prod'
