import code
import os
import glob
import sys

from unittest import TestCase
from threading import Event, Thread

from manhattan.server import Server, main as server_main
from manhattan.client import (ServerError, TimeoutError, Client,
                              main as client_main)
from manhattan.log.timerotating import TimeRotatingLog

from . import data


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

        sys.argv = ['manhattan-server',
                    '--path=%s' % path]

        killed_event = Event()
        th = Thread(target=server_main, args=(killed_event,))
        th.start()

        orig_interact = code.interact

        def fake_interact(banner, local):
            client = local['client']
            self.assertEqual(client.count('add to cart'), 5)
            self.assertEqual(client.count('began checkout'), 4)
            self.assertEqual(client.count('viewed page'), 6)

        code.interact = fake_interact

        try:
            client_main()
        finally:
            code.interact = orig_interact
            killed_event.set()
