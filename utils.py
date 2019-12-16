from datetime import datetime
import pytz
import json

from common_settings import *

class JSONEncoder(json.JSONEncoder):
    """
    A JSON encoder to support encode datetime
    """
    def default(self,obj):
        if isinstance(obj,datetime):
            return {
                "_type":"datetime",
                "value":obj.astimezone(tz=TZ).strftime("%Y-%m-%d %H:%M:%S.%f")
            }
        return json.JSONEncoder.default(self,obj)

class JSONDecoder(json.JSONDecoder):
    """
    A JSON decoder to support decode datetime
    """
    def __init__(self, *args, **kwargs):
        json.JSONDecoder.__init__(self, object_hook=self.object_hook, *args, **kwargs)

    def object_hook(self, obj):
        if '_type' not in obj:
            return obj
        type = obj['_type']
        if type == 'datetime':
            return datetime.strptime(obj["value"],"%Y-%m-%d %H:%M:%S.%f").replace(tzinfo=TZ)
        else:
            return obj

