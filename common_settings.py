import pytz
from env import env
from datetime import datetime

TIME_ZONE = env("TIME_ZONE",'Australia/Perth')
TZ = datetime.now(tz=pytz.timezone(TIME_ZONE)).tzinfo

