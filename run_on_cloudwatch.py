import os
import boto3
import argparse
from datetime import datetime, timedelta
from analytics import start_process
import logging
from analytics.reports import get_intent_report, get_taxonomy_report


logger = logging.getLogger('run_on_cloudwatch')

def run(argv=None, log_group_env_key="AWS_LOG_GROUP", run_function=start_process, func_args={"mode": "cloudwatch"}):

    target_date = datetime.now() - timedelta(days=1)
    log_group_name = os.environ.get(log_group_env_key)
    adv_log_group_name = os.environ.get("AWS_ADV_LOG_GROUP")
    aws_access_key_id = os.environ.get("AWS_KEY_ID")
    aws_secret_access_key = os.environ.get("AWS_KEY_SECRET")
    region_name = os.environ.get("AWS_REGION")

    parser = argparse.ArgumentParser()
    parser.add_argument('--date',
                        dest='date_string',
                        help='Date to query',
                        default=target_date.strftime('%Y-%m-%d'),
                        required=False)
    parser.add_argument("--aws-log-group",
                        dest="log_group_name",
                        help="Log group name for AWS Cloudwatch",
                        default=log_group_name,
                        required=False)
    parser.add_argument("--aws-adv-log-group",
                        dest="adv_log_group_name",
                        help="Advertiser dashboard stats Log group name for AWS Cloudwatch",
                        default=adv_log_group_name,
                        required=False)
    parser.add_argument("--aws-access-key-id",
                        dest="aws_access_key_id",
                        help="Access key ID for AWS",
                        default=aws_access_key_id,
                        required=False)
    parser.add_argument("--aws-secret-access-key",
                        dest="aws_secret_access_key",
                        help="Access secret key for AWS",
                        default=aws_secret_access_key,
                        required=False)
    parser.add_argument("--aws-region",
                        dest="region_name",
                        help="AWS region name",
                        default=region_name,
                        required=False)

    args = parser.parse_args(argv)

    if args.aws_access_key_id and args.aws_secret_access_key:
        aws_client = boto3.client(
            "logs",
            aws_access_key_id=args.aws_access_key_id,
            aws_secret_access_key=args.aws_secret_access_key,
            region_name=args.region_name)
    else:
        # Used to start boto session with profile_name
        # boto3.setup_default_session(profile_name='staging')
        #
        aws_client = boto3.client("logs", region_name=args.region_name)

    kwargs = {
        "date_string": args.date_string,
        "log_group_name": args.log_group_name,
        "adv_log_group_name": args.adv_log_group_name,
        "aws_client": aws_client
    }

    run_function(**func_args, **kwargs)


def _validate_date_string(date_string):
    try:
        return date_string and datetime.strptime(date_string, '%Y-%m-%d')
    except ValueError as e:
        logger.info(f'Invalid date_str {date_string}')
        return False


def _validate_log_group(log_group):
    return isinstance(log_group, str) and len(log_group) > 1


def lambda_handler(event, context):
    logger.info('Staring analytics')

    date_str = event.get('date_string', None)
    log_group = event.get('log_group', None)
    args = []
    if _validate_date_string(date_str):
        args.extend(['--date', date_str])
    if _validate_log_group(log_group):
        args.extend(['--aws-log-group', log_group])

    run(args)
    return 'completed successfully'


if __name__ == '__main__':
    run()
    get_taxonomy_report()
    get_intent_report()
