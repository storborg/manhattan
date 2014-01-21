import code
import os
import os.path
import sys
import time

from threading import Event, Thread

from sqlalchemy import create_engine

from manhattan.server import Server, main as server_main, logging_config
from manhattan.client import (ServerError, TimeoutError, Client,
                              main as client_main)
from manhattan.log.timerotating import TimeRotatingLog

from . import data
from .base import BaseTest, work_path
from .test_combinations import drop_existing_tables


class MockBackend(object):

    def foo(self, a, b):
        return u"foo: %r %r" % (a, b)

    def bar(self, *args, **kw):
        return u"bar: %r %r" % (args, kw)

    def failme(self, a):
        raise ValueError('sad')


class TestClientServer(BaseTest):

    def test_basic(self):
        backend = MockBackend()
        server = Server(backend, 'tcp://127.0.0.1:31338')
        client = Client('tcp://127.0.0.1:31338')

        try:
            server.start()
            # This test is screwy. If simplejson is installed, it will convert
            # all strings to str objects. Without it, you get unicode objects.
            # We require simplejson so assume it's gonna be str.
            self.assertEqual(client.foo(4, 'blah'), "foo: 4 'blah'")
            self.assertEqual(
                client.bar('hello', 'world', **dict(a=12, b='blah')),
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

    def _run_server_with_args(self, args, path, url, bind):
        log = TimeRotatingLog(path)
        data.run_clickstream(log)

        drop_existing_tables(create_engine(url))

        sys.argv = args

        killed_event = Event()
        th = Thread(target=server_main, args=(killed_event,))
        orig_interact = code.interact

        try:
            th.start()

            # Give the server time to process all the records before querying
            # it.
            time.sleep(0.5)

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

            sys.argv = ['manhattan-client', '--connect=%s' % bind]
            client_main()
        finally:
            code.interact = orig_interact
            killed_event.set()
            # Wait for thread to die before returning.
            time.sleep(0.5)

    def test_clientserver_executable(self):
        path = work_path('clientserver-executable')
        log_path = work_path('debug.log')
        url = 'sqlite:////tmp/manhattan-clientserver.db'
        bind = 'tcp://127.0.0.1:5555'

        args = [
            'manhattan-server',
            '--url=%s' % url,
            '--path=%s' % path,
            '--log=%s' % log_path,
            '--bind=%s' % bind,
            '--complex="abandoned cart|add to cart|began checkout"',
            '--complex="abandoned checkout|began checkout|completed checkout"',
            '--complex="abandoned after validation failure|'
            'began checkout,checkout validation failed|completed checkout"',
            '--complex="abandoned after payment failure|'
            'began checkout,payment failed|completed checkout"',
        ]
        self._run_server_with_args(args, path, url, bind)
        self.assertTrue(os.path.exists(log_path))

    def test_configure_logging(self):
        cfg = logging_config(filename=None)
        self.assertNotIn('root_file', cfg)

    def test_clientserver_python_config(self):
        path = data.sampleconfig['input_log_path']
        url = data.sampleconfig['sqlalchemy_url']
        bind = data.sampleconfig['bind']
        log_path = data.sampleconfig['error_log_path']

        args = ['manhattan-server',
                '--config=manhattan.tests.data.sampleconfig']
        self._run_server_with_args(args, path, url, bind)
        self.assertTrue(os.path.exists(log_path))
