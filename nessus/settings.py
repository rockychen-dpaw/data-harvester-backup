from env import env
from common_settings import *

REDIS_HOST = env("REDIS_HOST",vtype=str,required=True)
REDIS_PORT = env("REDIS_PORT",6379)

NESSUS_BASE = env("NESSUS_BASE",vtype=str,required=True)
NESSUS_ACCESS_KEY = env("NESSUS_ACCESS_KEY",vtype=str,required=True)
NESSUS_SECRET_KEY = env("NESSUS_SECRET_KEY",vtype=str,required=True)
NESSUS_URL = env("NESSUS_URL",vtype=str,required=True)

AZURE_STORAGE_CONNECTION_STRING = env("AZURE_STORAGE_CONNECTION_STRING",vtype=str,required=True)
NESSUS_CONTAINER = env("NESSUS_CONTAINER",vtype=str,required=True)
