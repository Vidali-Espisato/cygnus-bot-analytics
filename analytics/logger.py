import logging

LOG_LEVEL = 'INFO'
LOG_FORMAT = '%(asctime)s %(levelname)s:%(name)s:%(message)s'
LOG_DATEFORMAT = '%Y-%m-%d %H:%M:%S'

logging.basicConfig(format=LOG_FORMAT, datefmt=LOG_DATEFORMAT, level=LOG_LEVEL)
