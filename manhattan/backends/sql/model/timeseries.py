from sqlalchemy import Table, Column, ForeignKey, types
from sqlalchemy.sql import select, func

from . import meta


rollups = (
    'all',
    604800,   # 1 week
    86400,    # 1 day
    3600,     # 1 hour
    60,       # 1 minute
)


conversion_tables = {}
impression_tables = {}
variant_conversion_tables = {}


for rollup in rollups:
    conv = Table('conversions_%s_bucket' % rollup,
                 meta.metadata,
                 Column('goal_id', None, ForeignKey('goals.id'),
                        primary_key=True, autoincrement=False),
                 Column('start_timestamp', types.Integer, primary_key=True,
                        autoincrement=False),
                 Column('count', types.Integer, nullable=False, default=0),
                 Column('value', types.Numeric(10, 2), nullable=True),
                 mysql_engine='InnoDB')
    conversion_tables[rollup] = conv

    impr = Table('impressions_%s_bucket' % rollup,
                 meta.metadata,
                 Column('variant_id', None, ForeignKey('variants.id'),
                        primary_key=True, autoincrement=False),
                 Column('start_timestamp', types.Integer, primary_key=True,
                        autoincrement=False),
                 Column('count', types.Integer, nullable=False, default=0),
                 mysql_engine='InnoDB')
    impression_tables[rollup] = impr

    varc = Table('variant_conversions_%s_bucket' % rollup,
                 meta.metadata,
                 Column('variant_id', None, ForeignKey('variants.id'),
                        primary_key=True, autoincrement=False),
                 Column('goal_id', None, ForeignKey('goals.id'),
                        primary_key=True, autoincrement=False),
                 Column('start_timestamp', types.Integer, primary_key=True),
                 Column('count', types.Integer, nullable=False, default=0),
                 Column('value', types.Numeric(10, 2), nullable=True),
                 mysql_engine='InnoDB')
    variant_conversion_tables[rollup] = varc


def bucket_for_timestamp(rollup, timestamp):
    """
    Given a timestamp and rollup, return the start_timestamp corresponding
    to the bucket containing the given timestamp.
    """
    if rollup not in rollups:
        raise ValueError('invalid rollup: %r' % rollup)
    if rollup == 'all':
        return 0
    return timestamp - (timestamp % rollup)


def filter_q(t, q, start, goal_id=None, variant_id=None):
    if goal_id:
        q = q.where(t.c.goal_id == goal_id)
    if variant_id:
        q = q.where(t.c.variant_id == variant_id)
    q = q.where(t.c.start_timestamp == start)
    return q


def increment(tables, timestamp, goal_id=None, variant_id=None, value=None):
    for rollup in rollups:
        start = bucket_for_timestamp(rollup, timestamp)
        t = tables[rollup]

        q = filter_q(t, select([t.c.count]), start, goal_id, variant_id)

        if not q.scalar():
            kw = {}
            if goal_id:
                kw['goal_id'] = goal_id
            if variant_id:
                kw['variant_id'] = variant_id
            if value:
                kw['value'] = value
            q = t.insert().values(count=1,
                                  start_timestamp=start,
                                  **kw)
        else:
            kw = {}
            if value:
                kw['value'] = t.c.value + value
            q = t.update().values(count=t.c.count + 1, **kw)
            q = filter_q(t, q, start, goal_id, variant_id)

        meta.Session.execute(q)


def record_conversion(goal_id, timestamp, value):
    increment(conversion_tables, timestamp, goal_id=goal_id, value=value)


def record_impression(variant_id, timestamp):
    increment(impression_tables, timestamp, variant_id=variant_id)


def record_variant_conversion(variant_id, goal_id, timestamp, value):
    increment(variant_conversion_tables, timestamp,
              goal_id=goal_id, variant_id=variant_id, value=value)


def aggregate(col, goal_id=None, variant_id=None, start=None, end=None):
    assert variant_id or goal_id, 'must specify variant_id or goal_id'

    if variant_id and goal_id:
        tables = variant_conversion_tables
    elif variant_id:
        tables = impression_tables
    else:
        tables = conversion_tables

    rollup = choose_rollup(start, end)

    t = tables[rollup]
    col = getattr(t.c, col)
    q = select([func.sum(col)])

    if goal_id:
        q = q.where(t.c.goal_id == goal_id)
    if variant_id:
        q = q.where(t.c.variant_id == variant_id)

    if start:
        q = q.where(t.c.start_timestamp >= start)
    if end:
        q = q.where(t.c.start_timestamp < end)

    return q.scalar()


def choose_rollup(start, end):
    if not start and not end:
        return 'all'

    assert start and end, 'must specify both start and end, or neither'

    limit = (end - start) * 0.01
    # Pick the largest rollup that's no larger than 1/10th of the range.
    for rollup in rollups[1:]:
        if rollup < limit:
            return rollup

    # Worst case, use the smallest rollup level we have.
    return rollups[-1]


def count(goal_id=None, variant_id=None, start=None, end=None):
    return aggregate('count', goal_id=goal_id, variant_id=variant_id,
                     start=start, end=end)


def total_value(goal_id, variant_id=None, start=None, end=None):
    return aggregate('value', goal_id=goal_id, variant_id=variant_id,
                     start=start, end=end)
