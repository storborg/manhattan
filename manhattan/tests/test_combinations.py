import logging

import os
import glob
import sys
from unittest import TestCase

from sqlalchemy import MetaData, create_engine
from sqlalchemy.exc import SAWarning
from webob import Request
import zmq

import warnings
# Filter out pysqlite Decimal loss of precision warning.
warnings.filterwarnings('ignore',
                        '.*pysqlite does \*not\* support Decimal',
                        SAWarning,
                        'sqlalchemy.types')

from manhattan import visitor
from manhattan.visitor import Visitor
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

    def _run_clickstream(self, log):
        visitors = {}

        def get_visitor(vid):
            if vid not in visitors:
                visitors[vid] = Visitor(vid, log)
            return visitors[vid]

        for action in data.test_clickstream:
            cmd = action[0]
            v = get_visitor(action[1])
            args = action[2:]

            if cmd == 'page':
                req = Request.blank(args[0])
                v.page(req)
            elif cmd == 'pixel':
                v.pixel()
            elif cmd == 'goal':
                value = args[1]
                v.goal(args[0],
                       value=value,
                       value_type=visitor.SUM if value else None,
                       value_format=visitor.CURRENCY if value else None)
            elif cmd == 'split':
                v.split(args[0])

    def _check_backend_queries(self, backend):
        self.assertEqual(backend.count('add to cart'), 4)
        self.assertEqual(backend.count('began checkout'), 3)
        self.assertEqual(backend.count('viewed page'), 5)

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
        self.assertEqual(len(sessions), 5)

    def _check_clickstream(self, log, backend):
        worker = Worker(log, backend)
        worker.run()
        self._check_backend_queries(backend)

    def test_memory_log(self):
        log = MemoryLog()
        self._run_clickstream(log)
        self._check_clickstream(log, MemoryBackend())

    def test_zeromq_log(self):
        ctx = zmq.Context()
        log_r = ZeroMQLog(ctx, 'r', stay_alive=False, endpoints='tcp://*:8128')
        log_w = ZeroMQLog(ctx, 'w')

        self._run_clickstream(log_w)
        self._check_clickstream(log_r, MemoryBackend())

    def test_sql_backend(self):
        log = MemoryLog()
        self._run_clickstream(log)
        url = 'sqlite:///'
        drop_existing_tables(create_engine(url))
        backend = SQLBackend(url, max_recent_visitors=1)
        self._check_clickstream(log, backend)

    def test_timerotating_log(self):
        path = '/tmp/manhattan-test-timelog'
        fnames = glob.glob('%s.[0-9]*' % path)
        for fname in fnames:
            os.remove(fname)

        log = TimeRotatingLog(path)
        self._run_clickstream(log)

        log.f.flush()

        log2 = TimeRotatingLog(path)
        self._check_clickstream(log2, MemoryBackend())

    def test_sql_worker_executable(self):
        path = '/tmp/manhattan-test-timelog'
        fnames = glob.glob('%s.[0-9]*' % path)
        for fname in fnames:
            os.remove(fname)

        log = TimeRotatingLog(path)
        self._run_clickstream(log)

        log.f.flush()

        sqlite_url = 'sqlite:////tmp/manhattan-test.sqlite'

        sys.argv = ['manhattan-worker',
                    '--url=%s' % sqlite_url,
                    '--path=%s' % path]

        worker_main()

        self._check_backend_queries(SQLBackend(sqlite_url))
