import code
import os
import os.path
import glob
import sys
import time

from unittest import TestCase
from threading import Event, Thread

from sqlalchemy import create_engine

from manhattan.server import Server, main as server_main, logging_config
from manhattan.client import (ServerError, TimeoutError, Client,
                              main as client_main)
from manhattan.log.timerotating import TimeRotatingLog

from . import data
from .test_combinations import drop_existing_tables


class MockBackend(object):

    def foo(self, a, b):
        return u"foo: %r %r" % (a, b)

    def bar(self, *args, **kw):
        return u"bar: %r %r" % (args, kw)

    def failme(self, a):
        raise ValueError('sad')


class TestClientServer(TestCase):

    def test_basic(self):
        backend = MockBackend()
        server = Server(backend, 'tcp://127.0.0.1:31338')
        client = Client('tcp://127.0.0.1:31338')

        try:
            server.start()
            self.assertEqual(client.foo(4, u'blah'),
                             "foo: 4 'blah'")
            self.assertEqual(
                client.bar(u'hello', u'world', **dict(a=12, b=u'blah')),
                "bar: ('hello', 'world') {'a': 12, 'b': 'blah'}")

            with self.assertRaisesRegexp(ServerError, 'ValueError: sad'):
                client.failme(42)
        finally:
            server.kill()

    def test_timeout(self):
        client = Client('tcp://127.0.0.1:31339', wait=10)
        with self.assertRaisesRegexp(TimeoutError,
                                     'Timed out after 10 ms waiting'):
            client.foo()

    def test_clientserver_executable(self):
        path = '/tmp/manhattan-test-timelog'
        fnames = glob.glob('%s.[0-9]*' % path)
        for fname in fnames:
            os.remove(fname)

        log = TimeRotatingLog(path)
        data.run_clickstream(log)

        url = 'mysql://manhattan:quux@localhost/manhattan_test'
        drop_existing_tables(create_engine(url))

        log_path = '/tmp/manhattan-debug.log'

        sys.argv = [
            'manhattan-server',
            '--url=%s' % url,
            '--path=%s' % path,
            '--log=%s' % log_path,
            '--bind=tcp://127.0.0.1:5555',
            '--complex="abandoned cart|add to cart|began checkout"',
            '--complex="abandoned checkout|began checkout|completed checkout"',
            '--complex="abandoned after validation failure|'
            'began checkout,checkout validation failed|completed checkout"',
            '--complex="abandoned after payment failure|'
            'began checkout,payment failed|completed checkout"',
        ]

        killed_event = Event()
        th = Thread(target=server_main, args=(killed_event,))
        orig_interact = code.interact

        try:
            th.start()

            # Give the server time to process all the records before querying
            # it.
            time.sleep(0.5)

            self.assertTrue(os.path.exists(log_path))

            def fake_interact(banner, local):
                client = local['client']
                self.assertEqual(client.count(u'add to cart', site_id=1), 5)
                self.assertEqual(client.count(u'began checkout', site_id=1), 4)
                self.assertEqual(client.count(u'viewed page', site_id=1), 6)
                self.assertEqual(client.count(u'abandoned cart', site_id=1), 1)
                self.assertEqual(
                    client.count(u'abandoned after validation failure',
                                 site_id=1), 0)

            code.interact = fake_interact

            sys.argv = ['manhattan-client',
                        '--connect=tcp://127.0.0.1:5555']
            client_main()
        finally:
            code.interact = orig_interact
            killed_event.set()

    def test_configure_logging(self):
        cfg = logging_config(filename=None)
        self.assertNotIn('root_file', cfg)
