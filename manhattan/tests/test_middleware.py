import re

from unittest import TestCase

from webob import Request, Response
from webtest import TestApp

from manhattan.middleware import ManhattanMiddleware
from manhattan.record import Record
from manhattan.log.memory import MemoryLog


class SampleApp(object):

    def __call__(self, environ, start_response):
        req = Request(environ)

        if req.path_info.endswith('.txt'):
            s = 'Hello %s' % req.path_info
            resp = Response(s)
            resp.content_type = 'text/plain'
        elif req.path_info.endswith('.iter'):
            resp = Response()
            s = 'Hello %s' % req.path_info

            def app_iter(sample):
                for piece in ('<html><body>', sample, '</body>', '</html>'):
                    yield piece
                self.consumed_iter = True
                yield ' '

            self.consumed_iter = False
            resp.content_type = 'text/html'
            resp.app_iter = app_iter(s)
        else:
            s = '<html><body><h1>Hello %s</h1></body></html>' % req.path_info
            resp = Response(s)
            resp.content_type = 'text/html'

        return resp(environ, start_response)


log = MemoryLog()

host_map = {'localhost': 3,
            'example.com': 5}

inner_app = SampleApp()
wrapped_app = ManhattanMiddleware(inner_app, log, host_map=host_map)
app = TestApp(wrapped_app)


class TestMiddleware(TestCase):

    def setUp(self):
        app.reset()
        log.purge()

    def process(self):
        records = list(log.process())
        self.assertEqual(len(records), 1)
        record = Record.from_list(records[0])
        return record

    def test_request(self):
        resp = app.get('/')

        record = self.process()
        self.assertEqual(record.key, 'page')
        self.assertEqual(record.site_id, '3')

        m = re.search('<img src="(.+)" alt="" />', resp.body)
        pixel_path = m.group(1)
        resp = app.get(pixel_path)
        self.assertEqual(resp.content_type, 'image/gif')

        record = self.process()
        self.assertEqual(record.key, 'pixel')
        self.assertEqual(record.site_id, '3')

        resp = app.get('/foo')

        record = self.process()
        self.assertEqual(record.key, 'page')
        self.assertTrue(record.url.endswith('/foo'))
        self.assertEqual(record.site_id, '3')

    def test_host_map(self):
        resp = app.get('/hello', extra_environ={'HTTP_HOST': 'example.com'})
        self.assertEqual(resp.content_type, 'text/html')

        record = self.process()
        self.assertEqual(record.key, 'page')
        self.assertTrue(record.url.endswith('/hello'))
        self.assertEqual(record.site_id, '5')

    def test_unknown_host(self):
        resp = app.get('/somepage',
                       extra_environ={'HTTP_HOST':
                                      'supercalifragilicious.com'})
        self.assertEqual(resp.content_type, 'text/html')

        record = self.process()
        self.assertEqual(record.key, 'page')
        self.assertTrue(record.url.endswith('/somepage'))
        self.assertEqual(record.site_id, '0')

    def test_pixel_req(self):
        resp = app.get('/vpixel.gif')
        self.assertEqual(resp.content_type, 'image/gif',
                         'An html response should have a pixel tag.')

    def test_non_html_pixel(self):
        resp = app.get('/non-html-page.txt')
        self.assertNotIn('/vpixel.gif', resp.body,
                         'A non-html response should not have a pixel tag.')

    def test_generator_response(self):
        req = Request.blank('/quux.iter')
        resp = req.get_response(wrapped_app)

        self.assertFalse(inner_app.consumed_iter,
                         'The generator response has been buffered by '
                         'middleware before instead of being returned as an '
                         'iterable.')
        self.assertIn('/vpixel.gif', resp.body)
        self.assertTrue(inner_app.consumed_iter)

    def test_latin1_user_agent(self):
        # Example user agent is latin1-encoded, so should be preserved.
        sample_ua = '\xc0 \xe0 hello'
        resp = app.get('/somepage',
                       extra_environ={'HTTP_USER_AGENT': sample_ua})
        record = self.process()
        self.assertEqual(record.user_agent, sample_ua.decode('latin1'))
