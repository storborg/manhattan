from unittest import TestCase
from tempfile import NamedTemporaryFile

from webob import Request

from manhattan.visitor import Visitor
from manhattan.worker import Worker
from manhattan.backends.memory import MemoryBackend
from manhattan.log.memory import MemoryLog
from manhattan.log.gz import GZEventLog

from . import data


class TestVisitor(TestCase):

    def setUp(self):
        tf = NamedTemporaryFile()
        self.log = GZEventLog(tf.name)
        self.backend = MemoryBackend()
        self.visitors = {}
        for vid in ('a', 'b', 'c'):
            self.visitors[vid] = Visitor(vid, self.log)

    def test_clickstream(self):
        for action in data.test_clickstream:
            cmd = action[0]
            v = self.visitors[action[1]]
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

        worker = Worker(self.log, self.backend)
        worker.run()

        self.assertEqual(self.backend.count('add to cart'), 2)
        self.assertEqual(self.backend.count('began checkout'), 1)
        self.assertEqual(self.backend.count('viewed page'), 3)

        sessions = self.backend.get_sessions(goal='add to cart')
        self.assertIn('a', sessions)
        self.assertIn('b', sessions)
        self.assertNotIn('c', sessions)

        sessions = self.backend.get_sessions(
            goal='add to cart',
            variant=('red checkout form', 'False'))
        self.assertEqual(len(sessions), 1)
        self.assertIn('b', sessions)

        sessions = self.backend.get_sessions(
            variant=('red checkout form', 'False'))
        self.assertEqual(len(sessions), 1)
        self.assertIn('b', sessions)

        num = self.backend.count('add to cart',
                                 variant=('red checkout form', 'False'))
        self.assertEqual(num, 1)

        sessions = self.backend.get_sessions()
        self.assertEqual(len(sessions), 3)
