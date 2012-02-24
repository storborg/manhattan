import logging

from collections import Counter, defaultdict
from decimal import Decimal

from manhattan import visitor

from .rollups import AllRollup, LocalDayRollup, BrowserRollup
from .cache import DeferredLRUCache
from .model import VisitorHistory, Test, Goal

from .persistence.sql import SQLPersistentStore

log = logging.getLogger(__name__)


default_rollups = {
    'all': AllRollup(),
    'pst_day': LocalDayRollup('America/Los_Angeles'),
    'browser': BrowserRollup(),
}


class Backend(object):

    def __init__(self, sqlalchemy_url, rollups=None,
                 flush_every=500, cache_size=2000):
        self.rollups = rollups or default_rollups

        self.store = store = SQLPersistentStore(sqlalchemy_url)

        self.visitors = DeferredLRUCache(get_backend=store.get_visitor_history,
                                         put_backend=store.put_visitor_history,
                                         max_size=cache_size)
        self.tests = DeferredLRUCache(get_backend=store.get_test,
                                      put_backend=store.put_test,
                                      max_size=cache_size)
        self.goals = DeferredLRUCache(get_backend=store.get_goal,
                                      put_backend=store.put_goal,
                                      max_size=cache_size)

        self.pointer = self.store.get_pointer()
        self.records_since_flush = 0
        self.flush_every = flush_every

        self.reset_counters()

    def get_pointer(self):
        return self.pointer

    def reset_counters(self):
        self.inc_conversions = Counter()
        self.inc_values = defaultdict(Decimal)

        self.inc_variant_conversions = Counter()
        self.inc_variant_values = Counter()

        self.inc_impressions = Counter()

    def handle(self, rec, ptr):
        try:
            history = self.visitors.get(rec.vid)
        except KeyError:
            history = VisitorHistory()

        if rec.key == 'pixel':
            history.nonbot = True
            for rec in history.nonbot_queue:
                self.handle_nonbot(rec, history)

        elif history.nonbot:
            self.handle_nonbot(rec, history)

        else:
            history.nonbot_queue.append(rec)

        self.visitors.put(rec.vid, history)
        self.pointer = ptr
        self.records_since_flush += 1

        if self.records_since_flush > self.flush_every:
            self.flush()
            self.records_since_flush = 0

    def handle_nonbot(self, rec, history):
        assert rec.key in ('page', 'goal', 'split')
        ts = int(float(rec.timestamp))

        if rec.key == 'page':
            history.ips.add(rec.ip)
            history.user_agents.add(rec.user_agent)
            self.record_conversion(history,
                                   vid=rec.vid,
                                   name=u'viewed page',
                                   timestamp=ts)

        elif rec.key == 'goal':
            self.record_conversion(history,
                                   vid=rec.vid,
                                   name=rec.name,
                                   timestamp=ts,
                                   value=rec.value,
                                   value_type=rec.value_type,
                                   value_format=rec.value_format)

        else:  # split
            self.record_impression(history,
                                   vid=rec.vid,
                                   name=rec.test_name,
                                   selected=rec.selected,
                                   timestamp=ts)

    def record_impression(self, history, vid, name, selected, timestamp):
        variant = name, selected
        history.variants.add(variant)

        try:
            test = self.tests.get(name)
        except KeyError:
            test = Test()
            test.first_timestamp = timestamp

        test.last_timestamp = timestamp

        self.tests.put(name, test)

        # Record this impression in appropriate time buckets both on the
        # history object and in the current incremental accumulators.
        for rollup_key, rollup in self.rollups.iteritems():
            bucket_id = rollup.get_bucket(timestamp, history)
            key = (name, selected, rollup_key, bucket_id)
            if key not in history.impression_keys:
                history.impression_keys.add(key)
                self.inc_impressions[key] += 1

    def record_conversion(self, history, vid, name, timestamp, value=None,
                          value_type='', value_format=''):
        try:
            goal = self.goals.get(name)
        except KeyError:
            goal = Goal()
            goal.value_type = value_type
            goal.value_format = value_format

        self.goals.put(name, goal)

        if value:
            value = Decimal(value)

        # Record this goal conversion in appropriate time buckets both on the
        # history object and in the current incremental accumulators.
        for rollup_key, rollup in self.rollups.iteritems():
            bucket_id = rollup.get_bucket(timestamp, history)

            conv_key = (name, rollup_key, bucket_id)
            if conv_key not in history.conversion_keys:
                history.conversion_keys.add(conv_key)
                self.inc_conversions[conv_key] += 1
            if value:
                self.inc_values[conv_key] += value

            for test_name, selected in history.variants:
                vc_key = (name, test_name, selected, rollup_key, bucket_id)
                if vc_key not in history.variant_conversion_keys:
                    history.variant_conversion_keys.add(vc_key)
                    self.inc_variant_conversions[vc_key] += 1
                if value:
                    self.inc_variant_values[vc_key] += value

    def flush(self):
        self.store.begin()

        self.visitors.flush()
        self.tests.flush()
        self.goals.flush()

        # Add local counter state onto existing persisted counters.
        self.store.increment_conversion_counters(self.inc_conversions,
                                                 self.inc_values)
        self.store.increment_impression_counters(self.inc_impressions)
        self.store.increment_variant_conversion_counters(
            self.inc_variant_conversions, self.inc_variant_values)
        self.reset_counters()

        self.store.update_pointer(self.pointer)
        self.store.commit()

    def count(self, goal=None, variant=None, rollup_key='all', bucket_id=0):
        assert goal or variant, "must specify goal or variant"

        if goal and variant:
            test_name, selected = variant
            key = goal, test_name, selected, rollup_key, bucket_id
            local = self.inc_variant_conversions[key]
            flushed = self.store.count_variant_conversions(*key)[0]
        elif goal:
            key = goal, rollup_key, bucket_id
            local = self.inc_conversions[key]
            flushed = self.store.count_conversions(*key)[0]
        else:
            # variant
            name, selected = variant
            key = name, selected, rollup_key, bucket_id
            local = self.inc_impressions[key]
            flushed = self.store.count_impressions(*key)

        return local + flushed

    def goal_value(self, goal, variant=None, rollup_key='all', bucket_id=0):
        if variant:
            test_name, selected = variant
            key = goal, test_name, selected, rollup_key, bucket_id
            local = self.inc_variant_values[key]
            flushed = self.store.count_variant_conversions(*key)[1]
        else:
            key = goal, rollup_key, bucket_id
            local = self.inc_values[key]
            flushed = self.store.count_conversions(*key)[1]
        value = local + Decimal(str(flushed))

        goal_obj = self.goals.get(goal)
        if goal_obj.value_type == visitor.SUM:
            return value

        elif goal_obj.value_type == visitor.AVERAGE:
            return value / self.count(goal, variant)

        else:
            # visitor.PER
            return value / self.count(u'viewed page', variant)
