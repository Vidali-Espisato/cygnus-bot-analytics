import os
import argparse
from datetime import datetime, timedelta
from analytics import start_process
from dotenv import load_dotenv


def run():

    target_date = datetime.now() - timedelta(days=1)
    logs_path = os.path.abspath(os.path.curdir)

    parser = argparse.ArgumentParser()
    parser.add_argument('--date',
                        dest='date_string',
                        help='Date to query',
                        default=target_date.strftime('%Y-%m-%d'),
                        required=False)
    parser.add_argument('--logs-path',
                        dest='logs_path',
                        help='Logs path',
                        default=logs_path,
                        required=True)

    args = vars(parser.parse_args())
    start_process(mode="local", **args)


if __name__ == '__main__':
    load_dotenv()
    run()
