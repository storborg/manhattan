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

    def get_bucket(self, timestamp, history):
        return time.mktime(self.start_date_for(timestamp).timetuple())


class AllRollup(object):

    def get_bucket(self, timestamp, history):
        return 0


class BrowserRollup(object):

    def browser_from_user_agent(self, user_agent):
        # FIXME This is a pretty naive and less useful implementation.
        if 'Chrome' in user_agent:
            return u'Chrome'
        elif 'Safari' in user_agent:
            return u'Safari'
        elif 'Firefox' in user_agent:
            return u'Firefox'
        elif 'MSIE' in user_agent:
            return u'IE'
        else:
            return u'Unknown'

    def get_bucket(self, timestamp, history):
        if history.user_agents:
            return self.browser_from_user_agent(list(history.user_agents)[0])
        else:
            return u'Unknown'
