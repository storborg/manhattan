from unittest import TestCase
from tempfile import NamedTemporaryFile

from webob import Request

from manhattan.visitor import Visitor
from manhattan.backends.memory import MemoryBackend
from manhattan.log.memory import MemoryLog

from . import data


class TestVisitor(TestCase):

    def setUp(self):
        self.log = MemoryLog()
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

        records = list(self.log.process())
        self.assertEqual(len(records), 23)
