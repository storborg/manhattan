from unittest import TestCase

from webob import Request, Response
from webtest import TestApp

from manhattan.middleware import ManhattanMiddleware
from manhattan.log.memory import MemoryLog


class SampleApp(object):

    def __call__(self, environ, start_response):
        req = Request(environ)
        s = 'Hello World (%s)' % req.path_info
        resp = Response(s)
        resp.content_type = 'text/plain'
        return resp(environ, start_response)


log = MemoryLog()

app = SampleApp()
app = ManhattanMiddleware(app, log)
app = TestApp(app)


class TestMiddleware(TestCase):

    def test_request(self):
        app.get('/')
