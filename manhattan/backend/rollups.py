import time
from datetime import datetime

import pytz


class LocalDayRollup(object):

    def __init__(self, tzname):
        self.tz = pytz.timezone(tzname)

    def start_date_for(self, timestamp):
        dt = datetime.utcfromtimestamp(timestamp).replace(tzinfo=pytz.utc)
        dt_local = dt.astimezone(self.tz).replace(tzinfo=None)
        return dt_local.date()

    def bucket_start(self, start_timestamp):
        return time.mktime(self.start_date_for(start_timestamp).timetuple())


class AllRollup(object):

    def bucket_start(self, start_timestamp):
        return 0
