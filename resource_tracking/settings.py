import socket
import logging

from common_settings import *
from utils import parse_db_connection_string,classproperty
from db.database import PostgreSQL

import psycopg2

DATABASE = PostgreSQL(env("RESOURCE_TRACKING_DATABASE_URL",vtype=str,required=True))

AZURE_CONNECTION_STRING = env("RESOURCE_TRACKING_STORAGE_CONNECTION_STRING",vtype=str,required=True)
AZURE_CONTAINER = env("RESOURCE_TRACKING_CONTAINER",vtype=str,required=True)

