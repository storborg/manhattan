import types
import os
import glob
import time

from unittest import TestCase
from threading import Thread

from manhattan.record import PageRecord, Record
from manhattan.log.timerotating import TimeRotatingLog


def set_fake_name(log, index):
    def fake_name(self, timestamp):
        return '%s.%s' % (self.path, index)
    log.log_name_for = types.MethodType(fake_name, log, TimeRotatingLog)


def make_thread_consumer(log_r):
    consumed = []
    log_r.sleep_delay = 0.001

    def consume(l):
        for rec in l.process(stay_alive=True):
            consumed.append(Record.from_list(rec))

    consumer = Thread(target=consume, args=(log_r,))
    consumer.start()
    return consumed, consumer


class TimeRotatingLogTest(TestCase):

    def setUp(self):
        # Flush any old log files.
        for path in ('/tmp/manhattan-test-trl-basic',
                     '/tmp/manhattan-test-trl-mult',
                     '/tmp/manhattan-test-trl-stayalive',
                     '/tmp/manhattan-test-trl-stayalive-mult'):
            for fn in glob.glob('%s.[0-9]*' % path):
                os.remove(fn)

    def test_basic(self):
        log_w = TimeRotatingLog('/tmp/manhattan-test-trl-basic')
        log_w.write(PageRecord(url='/foo').to_list())

        log_w.f.flush()

        log_r = TimeRotatingLog('/tmp/manhattan-test-trl-basic')
        records = list(log_r.process(stay_alive=False))
        self.assertEqual(len(records), 1)
        rec = Record.from_list(records[0])
        self.assertEqual(rec.url, '/foo')

    def test_multiple_logs(self):
        log_w = TimeRotatingLog('/tmp/manhattan-test-trl-mult')

        set_fake_name(log_w, '001')
        log_w.write(PageRecord(url='/foo').to_list())

        set_fake_name(log_w, '004')
        log_w.write(PageRecord(url='/bar').to_list())

        log_w.f.flush()

        log_r = TimeRotatingLog('/tmp/manhattan-test-trl-mult')
        records = list(log_r.process(stay_alive=False))
        self.assertEqual(len(records), 2)
        self.assertEqual(Record.from_list(records[0]).url, '/foo')
        self.assertEqual(Record.from_list(records[1]).url, '/bar')

    def test_stay_alive_single(self):
        log_r = TimeRotatingLog('/tmp/manhattan-test-trl-stayalive')
        log_r.sleep_delay = 0.001
        consumed, consumer = make_thread_consumer(log_r)

        try:
            self.assertEqual(len(consumed), 0)

            log_w = TimeRotatingLog('/tmp/manhattan-test-trl-stayalive')

            log_w.write(PageRecord(url='/baz').to_list())
            log_w.f.flush()
            time.sleep(log_r.sleep_delay * 10)

            self.assertEqual(len(consumed), 1)
            self.assertEqual(consumed[0].url, '/baz')

            log_w.write(PageRecord(url='/herp').to_list())
            log_w.f.flush()
            time.sleep(log_r.sleep_delay * 10)

            self.assertEqual(len(consumed), 2)
            self.assertEqual(consumed[1].url, '/herp')
        finally:
            log_r.is_alive = False

    def test_stay_alive_multiple(self):
        log_r = TimeRotatingLog('/tmp/manhattan-test-trl-stayalive')
        log_r.sleep_delay = 0.001
        consumed, consumer = make_thread_consumer(log_r)

        try:
            self.assertEqual(len(consumed), 0)

            log_w = TimeRotatingLog('/tmp/manhattan-test-trl-stayalive')

            set_fake_name(log_w, '357')
            log_w.write(PageRecord(url='/baz').to_list())
            log_w.f.flush()
            time.sleep(log_r.sleep_delay * 10)

            self.assertEqual(len(consumed), 1)
            self.assertEqual(consumed[0].url, '/baz')

            set_fake_name(log_w, '358')
            log_w.write(PageRecord(url='/herp').to_list())
            log_w.f.flush()
            time.sleep(log_r.sleep_delay * 10)

            self.assertEqual(len(consumed), 2)
            self.assertEqual(consumed[1].url, '/herp')
        finally:
            log_r.is_alive = False
