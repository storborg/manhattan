from sqlalchemy import Table, Column, ForeignKey, types
from sqlalchemy.sql import select

from . import meta, custom_types


recent_visitors_table = Table(
    'recent_visitors',
    meta.metadata,
    Column('visitor_id', None, ForeignKey('visitors.visitor_id'),
           primary_key=True),
    Column('last_timestamp', types.Integer, nullable=False, index=True),
    Column('last_ip', custom_types.IP, nullable=False),
    mysql_engine='InnoDB')


def record_recent(ts, vid, ip):
    """
    Update the recent visitors table with this request information. Return True
    if we added a new record to the recent table, False otherwise.
    """
    t = recent_visitors_table
    q = select([t.c.visitor_id]).where(t.c.visitor_id == vid)
    if q.scalar():
        q = t.update().values(last_timestamp=ts, last_ip=ip)
        meta.Session.execute(q)
        return False
    else:
        q = t.insert().values(visitor_id=vid,
                              last_timestamp=ts,
                              last_ip=ip)
        meta.Session.execute(q)
        return True


def truncate_recent(max_records):
    """
    Count the number of records in the recent visitors table, and cut it down
    to ``max_records``. This doesn't lock in between doing the count and the
    delete, and solves the resulting race condition by erring on the side of
    deleting too few rows (leaving too many records behind). So, this function
    should not be counted on to make the table a particular size, just to keep
    it relatively around that size.
    """
    t = recent_visitors_table
    q = select([t.c.last_timestamp]).\
            order_by(t.c.last_timestamp.desc()).\
            limit(1).\
            offset(max_records)
    delete_before = q.scalar()
    if delete_before:
        q = t.delete().where(t.c.last_timestamp >= delete_before)
        meta.Session.execute(q)
