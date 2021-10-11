import asyncio
from run_on_cloudwatch import run
from analytics.reports import get_intent_report, get_taxonomy_report, get_urls_per_domain_report
from analytics.bidstream import process_bidstream, aggregate_n_days_records


def run_reports(*args, **kwargs):
    get_taxonomy_report()
    get_intent_report()
    get_urls_per_domain_report()

def run_bidstream():
    asyncio_run = lambda **kwargs: asyncio.run(process_bidstream(**kwargs))
    run(log_group_env_key="BIDSTREAM_LOG_GROUP", run_function=asyncio_run, func_args={"aggregate_for_n_days": 28})


if __name__ == '__main__':
    run_bidstream()
    run_reports()