from unittest import TestCase

from webob import Request
import zmq

from manhattan.visitor import Visitor
from manhattan.worker import Worker

from manhattan.log.memory import MemoryLog
from manhattan.log.gz import GZEventLog
from manhattan.log.zeromq import ZeroMQLog

from manhattan.backends.memory import MemoryBackend
from manhattan.backends.sql import SQLBackend

from . import data


class TestCombinations(TestCase):

    def _run_clickstream(self, log):
        visitors = {}
        for vid in ('a', 'b', 'c'):
            visitors[vid] = Visitor(vid, log)

        for action in data.test_clickstream:
            cmd = action[0]
            v = visitors[action[1]]
            args = action[2:]

            if cmd == 'page':
                req = Request.blank(args[0])
                v.page(req)
            elif cmd == 'pixel':
                v.pixel()
            elif cmd == 'goal':
                v.goal(args[0])
            elif cmd == 'split':
                v.split(args[0])

    def _check_clickstream(self, log, backend):
        worker = Worker(log, backend)
        worker.run()

        self.assertEqual(backend.count('add to cart'), 2)
        self.assertEqual(backend.count('began checkout'), 1)
        self.assertEqual(backend.count('viewed page'), 3)

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
        self.assertEqual(len(sessions), 3)

    def test_memory_log(self):
        log = MemoryLog()
        self._run_clickstream(log)
        self._check_clickstream(log, MemoryBackend())

    def test_gz_log(self):
        log = GZEventLog('/tmp/manhattan-test-log')
        self._run_clickstream(log)

        log2 = GZEventLog('/tmp/manhattan-test-log')
        self._check_clickstream(log2, MemoryBackend())

    def test_zeromq_log(self):
        ctx = zmq.Context()
        log_r = ZeroMQLog(ctx, 'r', stay_alive=False, endpoints='tcp://*:8128')
        log_w = ZeroMQLog(ctx, 'w')

        self._run_clickstream(log_w)
        self._check_clickstream(log_r, MemoryBackend())

    def test_sql_backend(self):
        log = MemoryLog()
        self._run_clickstream(log)
        backend = SQLBackend('sqlite:///')
        self._check_clickstream(log, backend)
