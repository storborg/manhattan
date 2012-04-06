from collections import Counter, defaultdict
from decimal import Decimal
from operator import itemgetter

from manhattan import visitor

from .rollups import AllRollup, LocalDayRollup, LocalWeekRollup, BrowserRollup
from .cache import DeferredLRUCache
from .model import VisitorHistory, Test, Goal

from .persistence.sql import SQLPersistentStore


default_rollups = {
    'all': AllRollup(),
    'pst_day': LocalDayRollup('America/Los_Angeles'),
    'pst_week': LocalWeekRollup('America/Los_Angeles'),
    'browser': BrowserRollup(),
}


class Backend(object):

    def __init__(self, sqlalchemy_url, rollups=None, complex_goals=None,
                 flush_every=500, cache_size=2000):
        self.rollups = rollups or default_rollups
        self.complex_goals = complex_goals or []

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
            # Limit nonbot queue to most recent 500 events.
            del history.nonbot_queue[:-500]

        self.visitors.put(rec.vid, history)
        self.pointer = ptr
        self.records_since_flush += 1

        if self.records_since_flush > self.flush_every:
            self.flush()
            self.records_since_flush = 0

    def handle_nonbot(self, rec, history):
        assert rec.key in ('page', 'goal', 'split')
        ts = int(float(rec.timestamp))
        site_id = int(rec.site_id)

        if rec.key == 'page':
            history.ips.add(rec.ip)
            history.user_agents.add(rec.user_agent)
            self.record_conversion(history,
                                   vid=rec.vid,
                                   name=u'viewed page',
                                   timestamp=ts,
                                   site_id=site_id)

        elif rec.key == 'goal':
            self.record_conversion(history,
                                   vid=rec.vid,
                                   name=rec.name,
                                   timestamp=ts,
                                   site_id=site_id,
                                   value=rec.value,
                                   value_type=rec.value_type,
                                   value_format=rec.value_format)

        else:  # split
            self.record_impression(history,
                                   vid=rec.vid,
                                   name=rec.test_name,
                                   selected=rec.selected,
                                   timestamp=ts,
                                   site_id=site_id)

    def record_impression(self, history, vid, name, selected, timestamp,
                          site_id):
        variant = name, selected
        history.variants.add(variant)

        try:
            test = self.tests.get(name)
        except KeyError:
            test = Test()
            test.first_timestamp = timestamp

        test.last_timestamp = timestamp
        test.variants.add(variant)

        self.tests.put(name, test)

        # Record this impression in appropriate time buckets both on the
        # history object and in the current incremental accumulators.
        for rollup_key, rollup in self.rollups.iteritems():
            bucket_id = rollup.get_bucket(timestamp, history)
            key = (name, selected, rollup_key, bucket_id, site_id)
            if key not in history.impression_keys:
                history.impression_keys.add(key)
                self.inc_impressions[key] += 1

    def iter_rollups(self, timestamp, history):
        for rollup_key, rollup in self.rollups.iteritems():
            bucket_id = rollup.get_bucket(timestamp, history)
            yield rollup_key, bucket_id

    def record_complex_goals(self, history, new_name, timestamp, site_id):
        for complex_name, include, exclude in self.complex_goals:
            # If all goals have now been satisfied in the 'include' set,
            # trigger a +1 delta on this complex goal in the current
            # rollups, and track that as a complex goal conversion in this
            # visitor history.
            if (new_name in include) and (history.goals >= include):
                new_keys = []
                for rollup_key, bucket_id in self.iter_rollups(timestamp,
                                                               history):
                    conv_key = (complex_name, rollup_key, bucket_id, site_id)
                    new_keys.append(conv_key)
                    self.inc_conversions[conv_key] += 1
                history.complex_keys[complex_name] = new_keys

            # If we are adding the first goal in the 'exclude' set, trigger
            # a -1 delta for all conversions on that complex goal in the
            # visitory history.
            if history.goals & exclude == set([new_name]):
                for key in history.complex_keys.pop(complex_name, []):
                    self.inc_conversions[key] -= 1

    def record_conversion(self, history, vid, name, timestamp, site_id,
                          value=None, value_type='', value_format=''):
        try:
            goal = self.goals.get(name)
        except KeyError:
            goal = Goal()
            goal.value_type = value_type
            goal.value_format = value_format

        self.goals.put(name, goal)

        if value:
            value = Decimal(value)

        if name not in history.goals:
            history.goals.add(name)
            # If this is a 'new' goal for this visitor, process complex
            # conversion goals.
            self.record_complex_goals(history, name, timestamp, site_id)

        # Record this goal conversion in appropriate time buckets both on the
        # history object and in the current incremental accumulators.
        for rollup_key, bucket_id in self.iter_rollups(timestamp, history):

            conv_key = (name, rollup_key, bucket_id, site_id)
            if conv_key not in history.conversion_keys:
                history.conversion_keys.add(conv_key)
                self.inc_conversions[conv_key] += 1
            if value:
                self.inc_values[conv_key] += value

            for test_name, selected in history.variants:
                vc_key = (name, test_name, selected, rollup_key, bucket_id,
                          site_id)
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

    def count(self, goal=None, variant=None, rollup_key='all', bucket_id=0,
              site_id=None):
        assert goal or variant, "must specify goal or variant"

        if goal and variant:
            test_name, selected = variant
            key = goal, test_name, selected, rollup_key, bucket_id, site_id
            local = self.inc_variant_conversions[key]
            flushed = self.store.count_variant_conversions(*key)[0]
        elif goal:
            key = goal, rollup_key, bucket_id, site_id
            local = self.inc_conversions[key]
            flushed = self.store.count_conversions(*key)[0]
        else:
            # variant
            name, selected = variant
            key = name, selected, rollup_key, bucket_id, site_id
            local = self.inc_impressions[key]
            flushed = self.store.count_impressions(*key)

        return local + flushed

    def goal_value(self, goal, variant=None, rollup_key='all', bucket_id=0,
                   site_id=None):

        goal_obj = self.goals.get(goal)

        if not goal_obj.value_type:
            return self.count(goal, variant, rollup_key=rollup_key,
                              bucket_id=bucket_id, site_id=site_id)

        if variant:
            test_name, selected = variant
            key = goal, test_name, selected, rollup_key, bucket_id, site_id
            local = self.inc_variant_values[key]
            flushed = self.store.count_variant_conversions(*key)[1]
        else:
            key = goal, rollup_key, bucket_id, site_id
            local = self.inc_values[key]
            flushed = self.store.count_conversions(*key)[1]
        value = local + Decimal(str(flushed))

        if goal_obj.value_type == visitor.SUM:
            return value

        elif goal_obj.value_type == visitor.AVERAGE:
            count = self.count(goal, variant, rollup_key=rollup_key,
                               bucket_id=bucket_id, site_id=site_id)
            return value / count if count > 0 else 0

        else:
            # visitor.PER
            count = self.count(u'viewed page', variant,
                               rollup_key=rollup_key,
                               bucket_id=bucket_id, site_id=site_id)
            return value / count if count > 0 else 0

    def all_tests(self):
        # Start with flushed.
        all = self.store.all_tests()
        # Update from unflushed (so that dirty entries overwrite the flushed).
        all.update(self.tests.entries)
        # Sort by last timestamp descending.
        all = [(name, test.first_timestamp, test.last_timestamp)
               for name, test in all.iteritems()]
        all.sort(key=itemgetter(2), reverse=True)
        return all

    def results(self, test_name, goals, site_id=None):
        # Return a dict: keys are populations, values are a list of values for
        # the goals specified.
        test = self.tests.get(test_name)

        ret = {}
        for variant in test.variants:
            values = []
            for goal in goals:
                values.append(self.goal_value(goal, variant, site_id=site_id))
            ret[variant[1]] = values

        return ret
