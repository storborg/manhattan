from unittest import TestCase
from sqlalchemy import MetaData, Column, types, orm, exc
from sqlalchemy.ext.declarative import declarative_base

from manhattan.backends.sql.model import custom_types, timeseries


becomes_sentinel = object()


class TestTypeAbstract(TestCase):

    def setUp(self):
        metadata = MetaData('sqlite://')
        Base = declarative_base(metadata=metadata)

        class Foo(Base):
            __tablename__ = 'test_dedupe'
            id = Column(types.Integer, primary_key=True)
            val = Column(self.custom_type)

        metadata.create_all()
        sm = orm.sessionmaker(bind=metadata.bind)
        self.sess = orm.scoped_session(sm)
        self.klass = Foo

    def store(self, val):
        inst = self.klass(val=val)
        self.sess.add(inst)
        self.sess.commit()
        return inst.id

    def check(self, val, becomes=becomes_sentinel):
        pk = self.store(val)
        if becomes == becomes_sentinel:
            becomes = val
        got = self.sess.query(self.klass).get(pk).val
        self.assertEquals(got, becomes)


class TestIPType(TestTypeAbstract):
    custom_type = custom_types.IP

    def test_localhost(self):
        self.check('::1', '127.0.0.1')
        self.check('127.0.0.1')

    def test_none(self):
        self.check(None)

    def test_normal(self):
        self.check('1.2.3.4')
        self.check('192.168.127.216')

    def test_invalid_ip_fails(self):
        with self.assertRaises(exc.StatementError):
            self.store('288.421.212.1')


class TestTimeSeries(TestCase):

    def test_bucket_for_invalid_granularity(self):
        with self.assertRaises(ValueError):
            timeseries.bucket_for_timestamp(1200, 1234)

    def test_bucket_for_timestamp(self):
        ts = 1327899798
        for gg, desired in  [('all', 0),
                             (604800, 1327536000),
                             (86400, 1327881600),
                             (3600, 1327899600)]:
            self.assertEqual(timeseries.bucket_for_timestamp(gg, ts), desired)
