import re

from unittest import TestCase

from webob import Request, Response
from webtest import TestApp

from manhattan.middleware import ManhattanMiddleware
from manhattan.log.memory import MemoryLog


class SampleApp(object):

    def __call__(self, environ, start_response):
        req = Request(environ)

        if req.path_info.endswith('.txt'):
            s = 'Hello %s' % req.path_info
            resp = Response(s)
            resp.content_type = 'text/plain'
        else:
            s = '<html><body><h1>Hello %s</h1></body></html>' % req.path_info
            resp = Response(s)
            resp.content_type = 'text/html'

        return resp(environ, start_response)


log = MemoryLog()

app = SampleApp()
app = ManhattanMiddleware(app, log)
app = TestApp(app)


class TestMiddleware(TestCase):

    def setUp(self):
        app.reset()

    def test_request(self):
        log.purge()
        resp = app.get('/')

        records = list(log.process())
        self.assertEqual(len(records), 1)
        record = records[0]
        self.assertEqual(record[0], 'page')

        m = re.search('<img src="(.+)" alt="" />', resp.body)
        pixel_path = m.group(1)
        resp = app.get(pixel_path)
        self.assertEqual(resp.content_type, 'image/gif')

        records = list(log.process())
        self.assertEqual(len(records), 1)
        record = records[0]
        self.assertEqual(record[0], 'pixel')

        resp = app.get('/foo')

        records = list(log.process())
        self.assertEqual(len(records), 1)
        record = records[0]
        self.assertEqual(record[0], 'page')
        self.assertTrue(record[3].endswith('/foo'))

    def test_pixel_req(self):
        resp = app.get('/vpixel.gif')
        self.assertEqual(resp.content_type, 'image/gif')

    def test_non_html_pixel(self):
        resp = app.get('/non-html-page.txt')
        self.assertNotIn('/vpixel.gif', resp.body)
