import warnings

from sqlalchemy import MetaData, Table, Column, types, create_engine, select
from sqlalchemy.sql import and_
from sqlalchemy.dialects import mysql


# Catch truncated data warnings. These are likely to happen on pickle types
# with a backend field that is not large enough, and they will break
# things horribly.
warnings.filterwarnings('error',
                        'Data truncated for column',
                        Warning,
                        'sqlalchemy.engine.default')


from ..model import Goal, Test


class LargePickleType(types.PickleType):

    def load_dialect_impl(self, dialect):
        if dialect.name == 'mysql':
            return dialect.type_descriptor(mysql.LONGBLOB)  # pragma: nocover
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
            Column('name', types.String(255), nullable=False, index=True),
            Column('first_timestamp', types.Integer, nullable=False),
            Column('last_timestamp', types.Integer, nullable=False),
            Column('variants', LargePickleType, nullable=False),
            mysql_engine='InnoDB')

        self.goal_table = Table(
            'goals',
            self.metadata,
            Column('id', types.Integer, primary_key=True),
            Column('name', types.String(255), nullable=False, index=True),
            Column('value_type', types.CHAR(1), nullable=False, default=''),
            Column('value_format', types.CHAR(1), nullable=False, default=''),
            mysql_engine='InnoDB')

        self.conversion_counts_table = Table(
            'conversion_counts',
            self.metadata,
            Column('name', types.String(255), primary_key=True),
            Column('rollup_key', types.String(255), primary_key=True),
            Column('bucket_id', types.String(255), primary_key=True),
            Column('site_id', types.Integer, primary_key=True),
            Column('count', types.Integer, nullable=False, default=0),
            Column('value', types.Float, nullable=False, default=0),
            mysql_engine='InnoDB')

        self.impression_counts_table = Table(
            'impression_counts',
            self.metadata,
            Column('name', types.String(255), primary_key=True),
            Column('selected', types.String(255), primary_key=True),
            Column('rollup_key', types.String(255), primary_key=True),
            Column('bucket_id', types.String(255), primary_key=True),
            Column('site_id', types.Integer, primary_key=True),
            Column('count', types.Integer, nullable=False, default=0),
            mysql_engine='InnoDB')

        self.variant_conversion_counts_table = Table(
            'variant_conversion_counts',
            self.metadata,
            Column('goal_name', types.String(100), primary_key=True),
            Column('test_name', types.String(100), primary_key=True),
            Column('selected', types.String(100), primary_key=True),
            Column('rollup_key', types.String(100), primary_key=True),
            Column('bucket_id', types.String(100), primary_key=True),
            Column('site_id', types.Integer, primary_key=True),
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

    def begin(self):
        return self.engine.begin()

    def commit(self):
        return self.engine.commit()

    def criteria_from_dict(self, table, key_dict):
        criteria = []
        for col, val in key_dict.iteritems():
            criteria.append(getattr(table.c, col) == val)
        if len(criteria) > 1:
            return and_(*criteria)
        else:
            return criteria[0]

    def put_kv(self, table, key_dict, value_dict, increment=False):
        whereclause = self.criteria_from_dict(table, key_dict)

        if increment:
            update_dict = {}
            for col, val in value_dict.iteritems():
                update_dict[col] = getattr(table.c, col) + val
        else:
            update_dict = value_dict

        q = table.update().values(**update_dict).where(whereclause)
        r = q.execute()
        if r.rowcount == 0:
            value_dict.update(key_dict)
            q = table.insert().values(**value_dict)
            q.execute()

    def put_visitor_history(self, histories):
        for vid, history in histories.iteritems():
            self.put_kv(self.history_table, {'vid': vid}, {'history': history})

    def put_test(self, tests):
        for name, test in tests.iteritems():
            self.put_kv(self.tests_table,
                        {'name': name},
                        {'first_timestamp': test.first_timestamp,
                         'last_timestamp': test.last_timestamp,
                         'variants': test.variants})

    def put_goal(self, goals):
        for name, goal in goals.iteritems():
            self.put_kv(self.goal_table,
                        {'name': name},
                        {'value_type': goal.value_type,
                         'value_format': goal.value_format})

    def increment_conversion_counters(self, inc_conversions, inc_values):
        """
        Given a map of (goal name, rollup key, bucket start) tuples to
        tuples of (integer counts, Decimal values), adjust the state of the
        counts in the SQL database.
        """
        for key in set(inc_conversions) | set(inc_values):
            delta = inc_conversions.get(key, 0)
            value = inc_values.get(key, 0)
            name, rollup_key, bucket_id, site_id = key

            self.put_kv(self.conversion_counts_table,
                        {'name': name,
                         'rollup_key': rollup_key,
                         'bucket_id': bucket_id,
                         'site_id': site_id},
                        {'count': delta,
                         'value': value},
                        increment=True)

    def increment_impression_counters(self, inc_impressions):
        for key, delta in inc_impressions.iteritems():
            name, selected, rollup_key, bucket_id, site_id = key

            self.put_kv(self.impression_counts_table,
                        {'name': name,
                         'selected': selected,
                         'rollup_key': rollup_key,
                         'bucket_id': bucket_id,
                         'site_id': site_id},
                        {'count': delta},
                        increment=True)

    def increment_variant_conversion_counters(self, inc_variant_conversions,
                                              inc_variant_values):
        for key in set(inc_variant_conversions) | set(inc_variant_values):
            delta = inc_variant_conversions.get(key, 0)
            value = inc_variant_values.get(key, 0)
            goal_name, test_name, selected, \
                rollup_key, bucket_id, site_id = key

            self.put_kv(self.variant_conversion_counts_table,
                        {'goal_name': goal_name,
                         'test_name': test_name,
                         'selected': selected,
                         'rollup_key': rollup_key,
                         'bucket_id': bucket_id,
                         'site_id': site_id},
                        {'count': delta,
                         'value': value},
                        increment=True)

    def get_kv(self, table, get_cols, key_dict, default=None):
        to_select = [getattr(table.c, col) for col in get_cols]
        whereclause = self.criteria_from_dict(table, key_dict)

        r = select(to_select).where(whereclause).execute().first()
        if r:
            return r
        else:
            if default:
                return default
            else:
                raise KeyError

    def get_visitor_history(self, vid):
        r = self.get_kv(self.history_table,
                        ['history'],
                        {'vid': vid})
        return r[0]

    def get_test(self, name):
        r = self.get_kv(self.tests_table,
                        ['first_timestamp', 'last_timestamp', 'variants'],
                        {'name': name})
        first, last, variants = r
        return Test(first_timestamp=first,
                    last_timestamp=last,
                    variants=variants)

    def get_goal(self, name):
        r = self.get_kv(self.goal_table,
                        ['value_type', 'value_format'],
                        {'name': name})
        value_type, value_format = r
        return Goal(value_type=value_type, value_format=value_format)

    def count_conversions(self, name, rollup_key, bucket_id, site_id):
        return self.get_kv(self.conversion_counts_table,
                           ['count', 'value'],
                           {'name': name,
                            'rollup_key': rollup_key,
                            'bucket_id': bucket_id,
                            'site_id': site_id},
                           default=(0, 0))

    def count_impressions(self, name, selected, rollup_key, bucket_id,
                          site_id):
        r = self.get_kv(self.impression_counts_table,
                        ['count'],
                        {'name': name,
                         'selected': selected,
                         'rollup_key': rollup_key,
                         'bucket_id': bucket_id,
                         'site_id': site_id},
                        default=(0,))
        return r[0]

    def count_variant_conversions(self, goal_name, test_name, selected,
                                  rollup_key, bucket_id, site_id):
        return self.get_kv(self.variant_conversion_counts_table,
                           ['count', 'value'],
                           {'goal_name': goal_name,
                            'test_name': test_name,
                            'selected': selected,
                            'rollup_key': rollup_key,
                            'bucket_id': bucket_id,
                            'site_id': site_id},
                           default=(0, 0))

    def all_tests(self):
        t = self.tests_table
        r = select([t.c.name,
                    t.c.first_timestamp,
                    t.c.last_timestamp,
                    t.c.variants]).execute()
        ret = {}
        for name, first, last, variants in r:
            ret[name] = Test(first_timestamp=first,
                             last_timestamp=last,
                             variants=variants)
        return ret
