from unittest import TestCase
from tempfile import NamedTemporaryFile

from webob import Request

from manhattan.visitor import Visitor
from manhattan.storage import FakeBackend
from manhattan.log import EventLog


tf = NamedTemporaryFile()
log = EventLog(tf.name)
v = Visitor('deadbeef', log)


class TestVisitor(TestCase):

    def test_pageview(self):
        req = Request.blank('/hello')
        v.page(req)
