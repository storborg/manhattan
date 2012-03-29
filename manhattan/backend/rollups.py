"""
This module contains some example ``Rollup`` objects, each implementing the
interface expected by the Manhattan backend for rollup aggregations.
"""

import time
from datetime import datetime, timedelta

import pytz


class LocalRollup(object):

    def __init__(self, tzname):
        self.tz = pytz.timezone(tzname)

    def start_date_for(self, timestamp):
        dt = datetime.utcfromtimestamp(timestamp).replace(tzinfo=pytz.utc)
        dt_local = dt.astimezone(self.tz).replace(tzinfo=None)
        return dt_local.date()


class LocalDayRollup(LocalRollup):

    def get_bucket(self, timestamp, history):
        return time.mktime(self.start_date_for(timestamp).timetuple())


class LocalWeekRollup(LocalRollup):

    def get_bucket(self, timestamp, history):
        day = self.start_date_for(timestamp)
        days_from_sunday = day.isoweekday() % 7
        day -= timedelta(days=days_from_sunday)
        return time.mktime(day.timetuple())


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
        return (history.user_agents and
                self.browser_from_user_agent(list(history.user_agents)[0]) or
                u'')
