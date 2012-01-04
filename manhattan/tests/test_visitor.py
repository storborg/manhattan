from unittest import TestCase

from webob import Request

from manhattan.visitor import Visitor
from manhattan.storage import FakeBackend

backend = FakeBackend()
v = Visitor('deadbeef', backend)


class TestVisitor(TestCase):

    def test_pageview(self):
        req = Request.blank('/hello')
        v.pageview(req)
