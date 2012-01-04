from unittest import TestCase

from webob import Request, Response
from webtest import TestApp

from manhattan.middleware import ManhattanMiddleware
from manhattan.storage import FakeBackend


class SampleApp(object):

    def __call__(self, environ, start_response):
        req = Request(environ)
        s = 'Hello World'
        resp = Response(s)
        resp.content_type = 'text/plain'
        return resp(environ, start_response)


backend = FakeBackend()

app = SampleApp()
app = ManhattanMiddleware(app, backend)
app = TestApp(app)

class TestMiddleware(TestCase):

    def test_request(self):
        resp = app.get('/')
