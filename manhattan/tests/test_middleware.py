from unittest import TestCase
from tempfile import NamedTemporaryFile

from webob import Request, Response
from webtest import TestApp

from manhattan.middleware import ManhattanMiddleware
from manhattan.log import EventLog


class SampleApp(object):

    def __call__(self, environ, start_response):
        req = Request(environ)
        s = 'Hello World'
        resp = Response(s)
        resp.content_type = 'text/plain'
        return resp(environ, start_response)


tf = NamedTemporaryFile()
log = EventLog(tf.name)

app = SampleApp()
app = ManhattanMiddleware(app, log)
app = TestApp(app)


class TestMiddleware(TestCase):

    def test_request(self):
        resp = app.get('/')
