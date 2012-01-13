from unittest import TestCase
from tempfile import NamedTemporaryFile

from webob import Request

from manhattan.visitor import Visitor
from manhattan.worker import Worker
from manhattan.backends.memory import MemoryBackend

from manhattan.log.memory import MemoryLog
from manhattan.log.gz import GZEventLog

from . import data


class TestLogs(TestCase):

    def _run_clickstream(self, log):
        backend = MemoryBackend()

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

        worker = Worker(log, backend)
        worker.run()

        self.assertEqual(backend.count('add to cart'), 2)
        self.assertEqual(backend.count('began checkout'), 1)
        self.assertEqual(backend.count('viewed page'), 3)

    def test_memory_log(self):
        log = MemoryLog()
        self._run_clickstream(MemoryLog())

    def test_gz_log(self):
        log = GZEventLog('/tmp/manhattan-test-log')
        self._run_clickstream(log)
