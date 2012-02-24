import logging

import os
import glob
from unittest import TestCase
from decimal import Decimal

from sqlalchemy import MetaData, create_engine
from sqlalchemy.exc import SAWarning
import zmq

import warnings
# Filter out pysqlite Decimal loss of precision warning.
warnings.filterwarnings('ignore',
                        '.*pysqlite does \*not\* support Decimal',
                        SAWarning,
                        'sqlalchemy.types')
# Turn unicode bind param warnings into errors.
warnings.filterwarnings('error',
                        r'Unicode type received non-unicode bind',
                        SAWarning,
                        'sqlalchemy.engine.default')

from manhattan.worker import Worker

from manhattan.log.memory import MemoryLog
from manhattan.log.zeromq import ZeroMQLog
from manhattan.log.timerotating import TimeRotatingLog

from manhattan.backend import Backend

from . import data

logging.basicConfig()
logging.getLogger('sqlalchemy.engine').setLevel(logging.DEBUG)


def drop_existing_tables(engine):
    "Drop all tables, including tables that aren't defined in metadata."
    temp_metadata = MetaData()
    temp_metadata.reflect(bind=engine)
    for table in reversed(temp_metadata.sorted_tables):
        table.drop(bind=engine)


class TestCombinations(TestCase):

    def _check_backend_queries(self, backend):
        self.assertEqual(backend.count(u'add to cart'), 5)
        self.assertEqual(backend.count(u'began checkout'), 4)
        self.assertEqual(backend.count(u'viewed page'), 6)

        #sessions = backend.get_sessions(goal='add to cart')
        #self.assertIn('a', sessions)
        #self.assertIn('b', sessions)
        #self.assertNotIn('c', sessions)

        #sessions = backend.get_sessions(
        #    goal='completed checkout',
        #    variant=('red checkout form', 'False'))
        #self.assertEqual(len(sessions), 1)
        #self.assertIn('b', sessions)

        #sessions = backend.get_sessions(
        #    variant=('red checkout form', 'False'))
        #self.assertEqual(len(sessions), 1)
        #self.assertIn('b', sessions)

        num = backend.count(u'completed checkout',
                            variant=(u'red checkout form', u'False'))
        self.assertEqual(num, 3)

        #sessions = backend.get_sessions()
        #self.assertEqual(len(sessions), 6)

        revenue = backend.goal_value(u'completed checkout')
        self.assertEqual(revenue, Decimal('108.19'))

        revenue_nored = backend.goal_value(
            u'completed checkout',
            variant=(u'red checkout form', u'False'))
        self.assertEqual(revenue_nored, Decimal('108.19'))

        noreds = backend.count(variant=(u'red checkout form', u'False'))
        self.assertEqual(noreds, 3)

        margin = backend.goal_value(u'order margin')
        margin = margin.quantize(Decimal('.01'))
        self.assertEqual(margin, Decimal('23.47'))

        margin_per = backend.goal_value(u'margin per session')
        margin_per = margin_per.quantize(Decimal('.01'))
        self.assertEqual(margin_per, Decimal('3.90'))

        margin_per_noreds = backend.goal_value(
            u'margin per session',
            variant=(u'red checkout form', u'False'))
        margin_per_noreds = margin_per_noreds.quantize(Decimal('.01'))
        self.assertEqual(margin_per_noreds, Decimal('7.79'))

    def _get_backend(self, reset=False):
        #url = 'mysql://manhattan:quux@localhost/manhattan_test'
        url = 'sqlite:////tmp/manhattan-test.db'
        if reset:
            drop_existing_tables(create_engine(url))
        return Backend(url, flush_every=2, cache_size=5)

    def test_resume(self):
        path = '/tmp/manhattan-test-resume'
        fnames = glob.glob('%s.[0-9]*' % path)
        for fname in fnames:
            os.remove(fname)

        backend = self._get_backend(reset=True)

        log_w = TimeRotatingLog(path)
        data.run_clickstream(log_w, first=0, last=25)

        log_r1 = TimeRotatingLog(path)
        worker1 = Worker(log_r1, backend)
        worker1.run()

        first_pointer = backend.get_pointer()
        self.assertIsNotNone(first_pointer)

        backend = self._get_backend(reset=False)

        data.run_clickstream(log_w, first=25)
        log_r2 = TimeRotatingLog(path)
        worker2 = Worker(log_r2, backend)
        worker2.run(resume=True)

        second_pointer = backend.get_pointer()
        self.assertIsNotNone(second_pointer)

        self._check_backend_queries(backend)

    def test_basic(self):
        path = '/tmp/manhattan-test-basic'
        fnames = glob.glob('%s.[0-9]*' % path)
        for fname in fnames:
            os.remove(fname)

        backend = self._get_backend(reset=True)

        log_w = TimeRotatingLog(path)
        data.run_clickstream(log_w)

        log_r = TimeRotatingLog(path)
        worker1 = Worker(log_r, backend)
        worker1.run()

        self._check_backend_queries(backend)

    def test_memory_log(self):
        log = MemoryLog()
        data.run_clickstream(log)

        backend = self._get_backend(reset=True)

        worker1 = Worker(log, backend)
        worker1.run(resume=False)

        self._check_backend_queries(backend)

    def test_zeromq_log(self):
        ctx = zmq.Context()
        log_r = ZeroMQLog(ctx, 'r', stay_alive=False, endpoints='tcp://*:8128')
        log_w = ZeroMQLog(ctx, 'w')
        data.run_clickstream(log_w)

        backend = self._get_backend(reset=True)

        worker1 = Worker(log_r, backend)
        worker1.run(resume=False)

        self._check_backend_queries(backend)
