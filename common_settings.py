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
        'console': {'format': '%(asctime)s %(name)-12s %(message)s'},
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
        },
        'storage': {
            'handlers': ['console'],
            'level': 'DEBUG' if DEBUG else 'WARNING',
        },
        'db': {
            'handlers': ['console'],
            'level': 'DEBUG' if DEBUG else 'WARNING',
        }
    }
}
logging.config.dictConfig(LOG_CONFIG)
