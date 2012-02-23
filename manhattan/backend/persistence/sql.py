import logging
import warnings

import cPickle as pickle
from decimal import Decimal

from sqlalchemy import MetaData, Table, Column, types, create_engine, select
from sqlalchemy.sql import and_
from sqlalchemy.dialects import mysql


# Catch truncated data warnings.
warnings.filterwarnings('error',
                        'Data truncated for column',
                        Warning,
                        'sqlalchemy.engine.default')


from ..model import Goal, Test

log = logging.getLogger(__name__)


class LargePickleType(types.PickleType):

    def load_dialect_impl(self, dialect):
        if dialect.name == 'mysql':
            return dialect.type_descriptor(mysql.LONGBLOB)
        else:
            return dialect.type_descriptor(types.LargeBinary)


class SQLPersistentStore(object):

    def __init__(self, sqlalchemy_url):
        self.engine = create_engine(sqlalchemy_url, pool_recycle=3600,
                                    strategy='threadlocal')
        self.metadata = MetaData(bind=self.engine)

        self.pointer_table = Table(
            'pointer',
            self.metadata,
            Column('pointer', types.String(255), primary_key=True),
            mysql_engine='InnoDB')

        self.history_table = Table(
            'visitor_histories',
            self.metadata,
            Column('vid', types.String(40), primary_key=True),
            Column('history', LargePickleType, nullable=False),
            mysql_engine='InnoDB')

        self.tests_table = Table(
            'tests',
            self.metadata,
            Column('id', types.Integer, primary_key=True),
            Column('name', types.Unicode(255), nullable=False, index=True),
            Column('first_timestamp', types.Integer, nullable=False),
            Column('last_timestamp', types.Integer, nullable=False),
            mysql_engine='InnoDB')

        self.goal_table = Table(
            'goals',
            self.metadata,
            Column('id', types.Integer, primary_key=True),
            Column('name', types.Unicode(255), nullable=False, index=True),
            Column('value_type', types.CHAR(1), nullable=False, default=''),
            Column('value_format', types.CHAR(1), nullable=False, default=''),
            mysql_engine='InnoDB')

        self.conversion_counts_table = Table(
            'conversion_counts',
            self.metadata,
            Column('name', types.Unicode(255), primary_key=True),
            Column('rollup_key', types.String(255), primary_key=True),
            Column('bucket_start', types.Integer, primary_key=True,
                   autoincrement=False),
            Column('count', types.Integer, nullable=False, default=0),
            Column('value', types.Float, nullable=False, default=0),
            mysql_engine='InnoDB')

        self.impression_counts_table = Table(
            'impression_counts',
            self.metadata,
            Column('name', types.Unicode(255), primary_key=True),
            Column('selected', types.Unicode(255), primary_key=True),
            Column('rollup_key', types.String(255), primary_key=True),
            Column('bucket_start', types.Integer, primary_key=True,
                   autoincrement=False),
            Column('count', types.Integer, nullable=False, default=0),
            mysql_engine='InnoDB')

        self.variant_conversion_counts_table = Table(
            'variant_conversion_counts',
            self.metadata,
            Column('goal_name', types.Unicode(255), primary_key=True),
            Column('test_name', types.Unicode(255), primary_key=True),
            Column('selected', types.Unicode(255), primary_key=True),
            Column('rollup_key', types.String(255), primary_key=True),
            Column('bucket_start', types.Integer, primary_key=True,
                   autoincrement=False),
            Column('count', types.Integer, nullable=False, default=0),
            Column('value', types.Float, nullable=False, default=0),
            mysql_engine='InnoDB')

        self.metadata.create_all()

    def update_pointer(self, ptr):
        if ptr is None:
            return
        q = self.pointer_table.update().values(pointer=ptr)
        r = q.execute()
        if r.rowcount == 0:
            q = self.pointer_table.insert().values(pointer=ptr)
            q.execute()

    def get_pointer(self):
        return select([self.pointer_table.c.pointer]).scalar()

    def get_visitor_history(self, vid):
        t = self.history_table
        r = select([t.c.history]).where(t.c.vid == vid).scalar()
        if r:
            return r
        else:
            raise KeyError

    def put_visitor_history(self, histories):
        t = self.history_table
        for vid, history in histories.iteritems():
            q = t.update().values(history=history).where(t.c.vid == vid)
            r = q.execute()
            if r.rowcount == 0:
                q = t.insert().values(vid=vid, history=history)
                q.execute()

    def get_test(self, name):
        t = self.tests_table
        r = select([t.c.first_timestamp, t.c.last_timestamp]).\
                where(t.c.name == name).execute().first()
        if r:
            first, last = r
            return Test(first_timestamp=first,
                        last_timestamp=last)
        else:
            raise KeyError

    def put_test(self, tests):
        t = self.tests_table
        for name, test in tests.iteritems():
            q = t.update().values(first_timestamp=test.first_timestamp,
                                  last_timestamp=test.last_timestamp).\
                    where(t.c.name == name)
            r = q.execute()
            if r.rowcount == 0:
                q = t.insert().values(name=name,
                                      first_timestamp=test.first_timestamp,
                                      last_timestamp=test.last_timestamp)
                q.execute()

    def get_goal(self, name):
        t = self.goal_table
        r = select([t.c.value_type, t.c.value_format]).\
                where(t.c.name == name).execute().first()
        if r:
            value_type, value_format = r
            return Goal(value_type=value_type, value_format=value_format)
        else:
            raise KeyError

    def put_goal(self, goals):
        t = self.goal_table
        for name, goal in goals.iteritems():
            q = t.update().values(value_type=goal.value_type,
                                  value_format=goal.value_format).\
                    where(t.c.name == name)
            r = q.execute()
            if r.rowcount == 0:
                q = t.insert().values(name=name,
                                      value_type=goal.value_type,
                                      value_format=goal.value_format)
                q.execute()

    def increment_conversion_counters(self, inc_conversions, inc_values):
        """
        Given a map of (goal name, rollup key, bucket start) tuples to
        tuples of (integer counts, Decimal values), adjust the state of the
        counts in the SQL database.
        """
        t = self.conversion_counts_table
        for key in set(inc_conversions) | set(inc_values):
            delta = inc_conversions.get(key, 0)
            value = inc_values.get(key, 0)
            name, rollup_key, bucket_start = key

            q = t.update().values(count=t.c.count + delta,
                                  value=t.c.value + value).\
                    where(and_(t.c.name == name,
                               t.c.rollup_key == rollup_key,
                               t.c.bucket_start == bucket_start))
            r = q.execute()

            if r.rowcount == 0:
                q = t.insert().values(name=name,
                                      rollup_key=rollup_key,
                                      bucket_start=bucket_start,
                                      count=delta,
                                      value=value)
                q.execute()

    def increment_impression_counters(self, inc_impressions):
        t = self.impression_counts_table
        for key, delta in inc_impressions.iteritems():
            name, selected, rollup_key, bucket_start = key

            q = t.update().values(count=t.c.count + delta).\
                    where(and_(t.c.name == name,
                               t.c.selected == selected,
                               t.c.rollup_key == rollup_key,
                               t.c.bucket_start == bucket_start))
            r = q.execute()

            if r.rowcount == 0:
                q = t.insert().values(name=name,
                                      selected=selected,
                                      rollup_key=rollup_key,
                                      bucket_start=bucket_start,
                                      count=delta)
                q.execute()

    def increment_variant_conversion_counters(self, inc_variant_conversions,
                                              inc_variant_values):
        t = self.variant_conversion_counts_table
        for key in set(inc_variant_conversions) | set(inc_variant_values):
            delta = inc_variant_conversions.get(key, 0)
            value = inc_variant_values.get(key, 0)
            goal_name, test_name, selected, rollup_key, bucket_start = key

            q = t.update().values(count=t.c.count + delta,
                                  value=t.c.value + value).\
                    where(and_(t.c.goal_name == goal_name,
                               t.c.test_name == test_name,
                               t.c.selected == selected,
                               t.c.rollup_key == rollup_key,
                               t.c.bucket_start == bucket_start))
            r = q.execute()

            if r.rowcount == 0:
                q = t.insert().values(goal_name=goal_name,
                                      test_name=test_name,
                                      selected=selected,
                                      rollup_key=rollup_key,
                                      bucket_start=bucket_start,
                                      count=delta,
                                      value=value)
                q.execute()

    def begin(self):
        return self.engine.begin()

    def commit(self):
        return self.engine.commit()

    def rollback(self):
        return self.engine.rollback()

    def count_conversions(self, name, rollup_key, bucket_start):
        t = self.conversion_counts_table
        q = select([t.c.count, t.c.value]).\
                where(and_(t.c.name == name,
                           t.c.rollup_key == rollup_key,
                           t.c.bucket_start == bucket_start))
        return q.execute().first() or (0, 0)

    def count_impressions(self, name, selected, rollup_key, bucket_start):
        t = self.impression_counts_table
        q = select([t.c.count]).\
                where(and_(t.c.name == name,
                           t.c.selected == selected,
                           t.c.rollup_key == rollup_key,
                           t.c.bucket_start == bucket_start))
        return q.scalar() or 0

    def count_variant_conversions(self, goal_name, test_name, selected,
                                  rollup_key, bucket_start):
        t = self.variant_conversion_counts_table
        q = select([t.c.count, t.c.value]).\
                where(and_(t.c.goal_name == goal_name,
                           t.c.test_name == test_name,
                           t.c.selected == selected,
                           t.c.rollup_key == rollup_key,
                           t.c.bucket_start == bucket_start))
        return q.execute().first() or (0, 0)
