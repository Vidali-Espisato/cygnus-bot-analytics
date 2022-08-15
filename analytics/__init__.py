import itertools
from datetime import timedelta
from operator import itemgetter
from statistics import mean
from urllib.parse import urlparse
import analytics.logger
from analytics import db
from analytics.models import *
from analytics.utils import *
import logging

logger = logging.getLogger('bot_analytics_runner')


# extract attributes from page crawled log line
def get_page_crawled_attributes(line):
    group = get_re_match_group(line, PAGE_CRAWLED_RE_PATTERN,
                               PAGE_CRAWLED_LOG_LINE_GROUP_LENGTH)
    return datetime.strptime(group[0].split(".")[0],
                             '%Y-%m-%d %H:%M:%S'), group[1], float(
        group[2]), int(group[3])


# extract crawler frequency from log stats line
def get_crawler_frequency(line):
    group = get_re_match_group(line, CRAWLER_FREQUENCY_RE_PATTERN,
                               CRAWLER_FREQUENCY_LOG_LINE_GROUP_LENGTH)
    return int(group[2])


# extract attributes from page crawl error log line
def get_page_crawl_error_attributes(line):
    group = get_re_match_group(line, PAGE_CRAWL_ERROR_RE_PATTERN,
                               PAGE_CRAWL_ERROR_LOG_LINE_GROUP_LENGTH)
    return datetime.strptime(group[0].split(".")[0],
                             '%Y-%m-%d %H:%M:%S'), group[1], group[2]


# extract score from recommendation engine log line
def get_recommendation_engine_attributes(line):
    group = get_re_match_group(line, RECOMMENDATION_ENGINE_RE_PATTERN,
                               RECOMMENDATION_LOG_LINE_GROUP_LENGTH)
    return float(group[2]), float(group[3]), float(group[4])


# read info log file
def get_info_logs_summary(info_logs_file_path=None,
                          info_lines=None,
                          *args,
                          **kwargs):
    page_crawled_attributes = []

    if info_logs_file_path:
        info_lines = read_lines_from_file(info_logs_file_path)

    for line in info_lines:
        if is_page_crawled_log_line(line):
            timestamp, url, page_load_speed, page_size = get_page_crawled_attributes(
                line)
            page_crawled_attributes.append(
                InfoItem(url, timestamp, page_load_speed, page_size))
    return page_crawled_attributes


def get_frequency_logs_summary(frequency_logs_file_path=None,
                               frequency_lines=None,
                               *args,
                               **kwargs):
    crawler_frequencies = []

    if frequency_logs_file_path:
        frequency_lines = read_lines_from_file(frequency_logs_file_path)

    grouped_messages = {}
    for line in frequency_lines:
        key = ':'.join(line.split(":")[:2])
        if key not in grouped_messages:
            grouped_messages[key] = []
        grouped_messages[key].append(line)

    for _, messages in sorted(grouped_messages.items()):
        crawler_frequencies.append(
            sum([
                get_crawler_frequency(line) for line in messages
                if is_log_stats_log_line(line)
            ]))

    return crawler_frequencies


# read error log file
def get_error_logs_summary(error_logs_file_path=None,
                           error_lines=None,
                           *args,
                           **kwargs):
    page_crawl_error_attributes = []

    if error_logs_file_path:
        error_lines = read_lines_from_file(error_logs_file_path)

    for line in error_lines:
        if is_page_crawl_error_log_line(line):
            timestamp, compliant_reason, url = get_page_crawl_error_attributes(
                line)
            page_crawl_error_attributes.append(
                ErrorItem(url, timestamp, compliant_reason))
    return page_crawl_error_attributes


# build page items from log attributes and return
def get_page_items(attributes):
    if not isinstance(attributes, list):
        raise ValueError('attributes must be list type')
    page_items = []
    for url, group in itertools.groupby(attributes, lambda x: x.url):
        group = list(group)
        first_crawled_item, last_crawled_item = min(group,
                                                    key=lambda x: x.timestamp), \
                                                max(group,
                                                    key=lambda x: x.timestamp)
        page_items.append(
            PageItem(url, len(group),
                     urlparse(url).netloc,
                     float(last_crawled_item.page_load_speed),
                     int(last_crawled_item.page_size),
                     first_crawled_item.timestamp, last_crawled_item.timestamp,
                     last_crawled_item.compliant,
                     last_crawled_item.non_compliance_reason))
    return page_items


# build domain items from page items and return
def get_domain_items(page_items, date):
    if not isinstance(page_items, list):
        raise ValueError('page_items must be list type')
    if not isinstance(date, datetime):
        raise ValueError('date must be datetime.datetime type')
    domain_items = []
    for domain, group in itertools.groupby(
            sorted(page_items, key=lambda x: x.domain), lambda x: x.domain):
        group = list(group)
        non_compliance_group = sorted(filter(lambda x: not x.compliant, group),
                                      key=lambda x: x.non_compliance_reason)

        non_compliant_reasons = []
        for non_compliance_reason, error_group in itertools.groupby(
                non_compliance_group, lambda x: x.non_compliance_reason):
            non_compliant_reasons.append({
                "reason": non_compliance_reason,
                "count": len(list(error_group))
            })

        domain_items.append(
            DomainItem(date, domain, len(group),
                       sum([_.visit_count for _ in group]),
                       mean([_.page_load_speed for _ in group]),
                       sum([_.page_size for _ in group]),
                       sum([1 for _ in group if _.compliant]),
                       sum([1 for _ in group if not _.compliant]),
                       non_compliant_reasons))
    return domain_items


# build overview item from domain and page items then return
def get_overview_item(domain_items, page_items, crawler_frequencies, date):
    if not isinstance(domain_items, list):
        raise ValueError('domain_items must be list type')
    if not isinstance(page_items, list):
        raise ValueError('page_items must be list type')
    if not isinstance(crawler_frequencies, list):
        raise ValueError('crawler_frequencies must be list type')
    if not isinstance(date, datetime):
        raise ValueError('date must be datetime.datetime type')
    urls_per_domain = mean([_.page_count for _ in domain_items])
    page_count = sum([_.page_count for _ in domain_items])
    speed_dict = {'fast': 0, 'medium': 0, 'slow': 0}
    for page in page_items:
        page_load_speed = page.page_load_speed
        if page_load_speed < 500:
            speed_dict['fast'] += 1
        elif 500 <= page_load_speed < 1500:
            speed_dict['medium'] += 1
        else:
            speed_dict['slow'] += 1

    # build non compliant reasons
    all_non_compliant_reasons = [
        reason for _ in domain_items for reason in _.non_compliance_reasons
    ]
    all_non_compliant_reasons_count = sum(
        [_.non_compliance_count for _ in domain_items])
    non_compliant_reasons_count = []
    for key, group in itertools.groupby(all_non_compliant_reasons,
                                        lambda x: x['reason']):
        group = list(group)
        if all_non_compliant_reasons_count > 0:
            non_compliant_reasons_count.append({
                "reason":
                    key,
                "count":
                    sum([_['count'] for _ in group])
            })

    return {
        "date":
            date,
        "page_count":
            page_count,
        "visit_count":
            sum([_.visit_count for _ in domain_items]),
        "urls_per_domain_mean":
            urls_per_domain,
        "total_page_size":
            sum([_.total_page_size for _ in domain_items]),
        "compliance_count":
            sum([_.compliance_count for _ in domain_items]),
        "non_compliance_count":
            all_non_compliant_reasons_count,
        "crawl_frequency":
            mean(crawler_frequencies),
        "avg_page_load_speed":
            sum([_.page_count * _.avg_page_load_speed
                 for _ in domain_items]) / page_count if page_count > 0 else 0,
        "page_load_speed_count":
            speed_dict,
        "non_compliance_reasons_count":
            non_compliant_reasons_count,
    }


def get_cloudwatch_logs(aws_client, log_group_name, date_string, filters):
    if not isinstance(filters, dict):
        raise ValueError("Invalid filters value")
    logs_ = {}
    start_time = datetime.strptime(date_string, DATE_FORMAT)
    end_time = start_time + timedelta(hours=23, minutes=59, seconds=59)
    logger.info(
        f'Getting logs from {start_time} to {end_time} on {log_group_name}')

    for filter_key, filter_string in filters.items():
        next_token = True
        responses = []

        while next_token:
            logger.info(f"querying {filter_key} logs")
            query_args = {
                "logGroupName": log_group_name,
                "startTime": int(start_time.timestamp() * 1000),
                "endTime": int(end_time.timestamp() * 1000),
                "filterPattern": filter_string,
                "limit": int(os.getenv('LOG_ITEMS_LIMIT', 10000))
            }

            if isinstance(next_token, str):
                logger.info("paginating...")
                query_args["nextToken"] = next_token

            response = aws_client.filter_log_events(**query_args)
            next_token = response.get("nextToken")

            responses.extend(
                event.get("message") for event in response.get("events"))

        logs_[f"{filter_key}_lines"] = responses

    return logs_


# get recommendation engine logs summary
def get_recommendation_engine_summary(re_lines=None):
    scores = []

    for line in re_lines:
        if is_recommendation_engine_log_line(line):
            top1, top10, top50 = get_recommendation_engine_attributes(line)
            scores.append((top1, top10, top50,))
    return scores


# build advertiser dashboard stats item and return
def get_advertiser_dashboard_stats_item(stats, date):
    if not stats:
        return
    if not isinstance(stats, list):
        return
    if not isinstance(date, datetime):
        return
    search_count = len(stats)
    top1_avg = mean(map(itemgetter(0), stats))
    top10_avg = mean(map(itemgetter(1), stats))
    top50_avg = mean(map(itemgetter(2), stats))
    return {
        "search_count": search_count,
        "top1_avg": top1_avg,
        "top10_avg": top10_avg,
        "top50_avg": top50_avg,
        "date": date
    }


# start analytics process
def start_process(mode="local",
                  date_string=None,
                  logs_path=None,
                  log_group_name=None,
                  aws_client=None,
                  adv_log_group_name=None):
    args, adv_args = {}, {}
    if db.get_overview_doc_from_db(datetime.strptime(date_string, DATE_FORMAT)):
        logger.info(f'Overview document already exists for {date_string}')
        return
    if mode == "local":
        logger.info("running in local mode")
        args["info_logs_file_path"] = args[
            "frequency_logs_file_path"] = get_log_file_path(
            date_string, logs_path)
        args["error_logs_file_path"] = get_log_file_path(
            date_string, logs_path, log_level=LogLevel.ERROR)

    elif mode == "cloudwatch":
        logger.info("running in cloudwatch mode")
        filters = {
            "info": "INFO PAGE_CRAWLED",
            "frequency": "INFO Crawled",
            "error": "ERROR PAGE_CRAWL_ERROR"
        }
        args = get_cloudwatch_logs(aws_client, log_group_name, date_string,
                                   filters)
        adv_filters = {
            "re": "INFO recommendation_engine"
        }
        adv_args = get_cloudwatch_logs(aws_client, adv_log_group_name,
                                       date_string, adv_filters)

    page_crawled_attributes = get_info_logs_summary(**args)
    crawler_frequencies = get_frequency_logs_summary(**args)
    page_crawl_error_attributes = get_error_logs_summary(**args)

    all_page_items = get_page_items(page_crawled_attributes +
                                    page_crawl_error_attributes)
    stats = get_recommendation_engine_summary(**adv_args)
    stats_item = get_advertiser_dashboard_stats_item(stats, datetime.strptime(
        date_string, DATE_FORMAT))
    if stats_item:
        db.create_advertiser_dashboard_stats_item(stats_item)
    if not all_page_items:
        logger.info('No logs found')
        return
    # write all_page_items to elastic mongodb
    db.create_or_update_pages_documents(all_page_items)

    domain_items = get_domain_items(all_page_items,
                                    datetime.strptime(date_string, '%Y-%m-%d'))
    # write domain_items to mongodb
    db.create_or_update_domains([_.to_dict() for _ in domain_items])

    overview_item = get_overview_item(
        domain_items, all_page_items, crawler_frequencies,
        datetime.strptime(date_string, '%Y-%m-%d'))
    # write overview to mongodb
    db.create_or_update_overview_document(overview_item)
