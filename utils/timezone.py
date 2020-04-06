from datetime import datetime as pdatetime

import common_settings as settings


def now():
    """
    Return the current time with configured timezone
    """
    return pdatetime.now(tz=settings.TZ)

def datetime(year,month=1,day=1,hour=0,minute=0,second=0,microsecond=0):
    return pdatetime(year,month,day,hour,minute,second,microsecond,tzinfo=settings.TZ)

