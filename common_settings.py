import pytz
import logging.config

from env import env
from datetime import datetime

DEBUG = env("DEBUG",False)
TIME_ZONE = env("TIME_ZONE",'Australia/Perth')
TZ = datetime.now(tz=pytz.timezone(TIME_ZONE)).tzinfo

logging.basicConfig(level="WARNING")

LOG_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'console': {'format':  '%(asctime)s %(levelname)-8s %(name)-15s %(message)s'},
    },
    'handlers': {
        'console': {
            'level': 'DEBUG' if DEBUG else 'WARNING',
            'class': 'logging.StreamHandler',
            'formatter': 'console'
        },
    },
    'loggers': {
        'resource_tracking': {
            'handlers': ['console'],
            'level': 'DEBUG' if DEBUG else 'WARNING',
            'propagate':False
        },
        'storage': {
            'handlers': ['console'],
            'level': 'DEBUG' if DEBUG else 'WARNING',
            'propagate':False
        },
        'db': {
            'handlers': ['console'],
            'level': 'DEBUG' if DEBUG else 'WARNING',
            'propagate':False
        }
    },
    'root':{
        'handlers': ['console'],
        'level': 'WARNING',
        'propagate':False
    }
}
logging.config.dictConfig(LOG_CONFIG)
