import logging

import os
import glob
import sys
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

from manhattan.worker import Worker, main as worker_main

from manhattan.log.memory import MemoryLog
from manhattan.log.zeromq import ZeroMQLog
from manhattan.log.timerotating import TimeRotatingLog

from manhattan.backends.memory import MemoryBackend
from manhattan.backends.sql import SQLBackend

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
        self.assertEqual(backend.count('add to cart'), 5)
        self.assertEqual(backend.count('began checkout'), 4)
        self.assertEqual(backend.count('viewed page'), 6)

        sessions = backend.get_sessions(goal='add to cart')
        self.assertIn('a', sessions)
        self.assertIn('b', sessions)
        self.assertNotIn('c', sessions)

        sessions = backend.get_sessions(
            goal='completed checkout',
            variant=('red checkout form', 'False'))
        self.assertEqual(len(sessions), 1)
        self.assertIn('b', sessions)

        sessions = backend.get_sessions(
            variant=('red checkout form', 'False'))
        self.assertEqual(len(sessions), 1)
        self.assertIn('b', sessions)

        num = backend.count('completed checkout',
                            variant=('red checkout form', 'False'))
        self.assertEqual(num, 1)

        sessions = backend.get_sessions()
        self.assertEqual(len(sessions), 6)

    def _check_clickstream(self, log, backend):
        worker = Worker(log, backend)
        worker.run(resume=False)
        self._check_backend_queries(backend)

    def test_memory_log(self):
        log = MemoryLog()
        data.run_clickstream(log)
        self._check_clickstream(log, MemoryBackend())

    def test_zeromq_log(self):
        ctx = zmq.Context()
        log_r = ZeroMQLog(ctx, 'r', stay_alive=False, endpoints='tcp://*:8128')
        log_w = ZeroMQLog(ctx, 'w')

        data.run_clickstream(log_w)
        self._check_clickstream(log_r, MemoryBackend())

    def test_sql_backend(self):
        log = MemoryLog()
        data.run_clickstream(log)
        url = 'sqlite:///'
        drop_existing_tables(create_engine(url))
        backend = SQLBackend(url, max_recent_visitors=1)
        self._check_clickstream(log, backend)

        revenue = backend.goal_value('completed checkout')
        self.assertEqual(revenue, Decimal('108.19'))

        revenue_nored = backend.goal_value(
            'completed checkout',
            variant=('red checkout form', 'False'))
        self.assertEqual(revenue_nored, Decimal('31.78'))

        noreds = backend.count(variant=('red checkout form', 'False'))
        self.assertEqual(noreds, 1)

        margin = backend.goal_value('order margin')
        margin = margin.quantize(Decimal('.01'))
        self.assertEqual(margin, Decimal('23.47'))

        margin_per = backend.goal_value('margin per session')
        margin_per = margin_per.quantize(Decimal('.01'))
        self.assertEqual(margin_per, Decimal('3.90'))

        margin_per_noreds = backend.goal_value(
            'margin per session',
            variant=('red checkout form', 'False'))
        self.assertEqual(margin_per_noreds, Decimal('7.15'))

        part1_adds = backend.count('add to cart',
                                   start=1, end=2000)
        self.assertEqual(part1_adds, 2)
        part2_adds = backend.count('add to cart',
                                   start=2001, end=9999)
        self.assertEqual(part2_adds, 3)

        part1_checkouts = backend.goal_value('completed checkout',
                                             start=1, end=4000)
        self.assertEqual(part1_checkouts, Decimal('31.78'))
        part2_checkouts = backend.goal_value('completed checkout',
                                             start=4001, end=9999)
        self.assertEqual(part2_checkouts, Decimal('76.41'))

    def test_timerotating_log(self):
        path = '/tmp/manhattan-test-timelog'
        fnames = glob.glob('%s.[0-9]*' % path)
        for fname in fnames:
            os.remove(fname)

        log = TimeRotatingLog(path)
        data.run_clickstream(log)

        log.f.flush()

        log2 = TimeRotatingLog(path)
        self._check_clickstream(log2, MemoryBackend())

    def test_sql_worker_executable(self):
        path = '/tmp/manhattan-test-timelog'
        fnames = glob.glob('%s.[0-9]*' % path)
        for fname in fnames:
            os.remove(fname)

        log = TimeRotatingLog(path)
        data.run_clickstream(log)

        log.f.flush()

        sqlite_url = 'sqlite:////tmp/manhattan-test.sqlite'

        sys.argv = ['manhattan-worker',
                    '--url=%s' % sqlite_url,
                    '--path=%s' % path]

        worker_main()

        self._check_backend_queries(SQLBackend(sqlite_url))

    def _run_resume(self, backend):
        path = '/tmp/manhattan-test-timelog'
        fnames = glob.glob('%s.[0-9]*' % path)
        for fname in fnames:
            os.remove(fname)

        log_w = TimeRotatingLog(path)
        data.run_clickstream(log_w, first=0, last=5)

        log_r1 = TimeRotatingLog(path)
        worker1 = Worker(log_r1, backend)
        worker1.run()

        first_pointer = backend.get_pointer()
        self.assertIsNotNone(first_pointer)

        data.run_clickstream(log_w, first=5)
        log_r2 = TimeRotatingLog(path)
        worker2 = Worker(log_r2, backend)
        worker2.run(resume=True)

        second_pointer = backend.get_pointer()
        self.assertIsNotNone(second_pointer)

        self._check_backend_queries(backend)

    def test_memory_resume(self):
        self._run_resume(MemoryBackend())

    def test_sql_resume(self):
        url = 'sqlite:////tmp/manhattan-test-sql-resume.sqlite'
        drop_existing_tables(create_engine(url))
        backend = SQLBackend(url)
        self._run_resume(backend)
