from sqlalchemy import Table, Column, types
from sqlalchemy.sql import select

from . import meta


pointer_table = Table(
    'pointer',
    meta.metadata,
    Column('pointer', types.String(255), primary_key=True),
    mysql_engine='InnoDB')


def update_pointer(s):
    if s is None:
        return
    q = pointer_table.update().values(pointer=s)
    r = meta.Session.execute(q)
    if r.rowcount == 0:
        q = pointer_table.insert().values(pointer=s)
        meta.Session.execute(q)


def get_pointer():
    return select([pointer_table.c.pointer]).scalar()
