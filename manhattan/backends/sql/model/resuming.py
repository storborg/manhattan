from sqlalchemy import Table, Column, types

from . import meta


pointer_table = Table(
    'pointer',
    meta.metadata,
    Column('pointer', types.String(255), primary_key=True),
    mysql_engine='InnoDB')


def update_pointer(s):
    q = pointer_table.update().values(pointer=s)
    meta.Session.execute(q)
