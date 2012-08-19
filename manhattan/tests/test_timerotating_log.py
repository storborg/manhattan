import types
import time

from threading import Thread

from manhattan.record import Record, PageRecord, GoalRecord
from manhattan.log.timerotating import TimeRotatingLog

from .base import BaseTest, work_path


def set_fake_name(log, index):
    def fake_name(self, timestamp):
        return '%s.%s' % (self.path, index)
    log.log_name_for = types.MethodType(fake_name, log)


def make_thread_consumer(log_r, process_from=None):
    consumed = []
    last_pointer_container = [None]
    log_r.sleep_delay = 0.001

    def consume(l):
        for rec, ptr in l.process(stay_alive=True, process_from=process_from):
            consumed.append(Record.from_list(rec))
            last_pointer_container[0] = ptr

    consumer = Thread(target=consume, args=(log_r,))
    consumer.start()
    return consumed, consumer, last_pointer_container


class TimeRotatingLogTest(BaseTest):

    def test_basic(self):
        path = work_path('trl-basic')
        log_w = TimeRotatingLog(path)
        log_w.write(PageRecord(url='/foo').to_list())

        log_r = TimeRotatingLog(path)
        records = list(log_r.process(stay_alive=False))
        self.assertEqual(len(records), 1)
        rec = Record.from_list(records[0][0])
        self.assertEqual(rec.url, '/foo')

    def test_multiple_logs(self):
        path = work_path('trl-multi')
        log_w = TimeRotatingLog(path)

        set_fake_name(log_w, '001')
        log_w.write(PageRecord(url='/foo').to_list())

        set_fake_name(log_w, '004')
        log_w.write(PageRecord(url='/bar').to_list())

        log_r = TimeRotatingLog(path)
        records = list(log_r.process(stay_alive=False))
        self.assertEqual(len(records), 2)
        self.assertEqual(Record.from_list(records[0][0]).url, '/foo')
        self.assertEqual(Record.from_list(records[1][0]).url, '/bar')

    def test_stay_alive_single(self):
        path = work_path('trl-stayalive')
        log_r = TimeRotatingLog(path)
        log_r.sleep_delay = 0.001
        consumed, consumer, _ = make_thread_consumer(log_r)

        try:
            self.assertEqual(len(consumed), 0)

            log_w = TimeRotatingLog(path)

            log_w.write(PageRecord(url='/baz').to_list())
            time.sleep(log_r.sleep_delay * 10)

            self.assertEqual(len(consumed), 1)
            self.assertEqual(consumed[0].url, '/baz')

            log_w.write(PageRecord(url='/herp').to_list())
            time.sleep(log_r.sleep_delay * 10)

            self.assertEqual(len(consumed), 2)
            self.assertEqual(consumed[1].url, '/herp')
        finally:
            log_r.killed.set()

    def test_stay_alive_multiple(self):
        path = work_path('trl-stayalive-multi')
        log_r = TimeRotatingLog(path)
        log_r.sleep_delay = 0.001
        consumed, consumer, _ = make_thread_consumer(log_r)

        try:
            self.assertEqual(len(consumed), 0)

            log_w = TimeRotatingLog(path)

            set_fake_name(log_w, '357')
            log_w.write(PageRecord(url='/baz').to_list())
            time.sleep(log_r.sleep_delay * 10)

            self.assertEqual(len(consumed), 1)
            self.assertEqual(consumed[0].url, '/baz')

            set_fake_name(log_w, '358')
            log_w.write(PageRecord(url='/herp').to_list())
            time.sleep(log_r.sleep_delay * 10)

            self.assertEqual(len(consumed), 2)
            self.assertEqual(consumed[1].url, '/herp')
        finally:
            log_r.killed.set()

    def test_stay_alive_nofiles(self):
        log_r = TimeRotatingLog(work_path('trl-stayalive-none'))
        log_r.sleep_delay = 0.001
        consumed, consumer, _ = make_thread_consumer(log_r)
        log_r.killed.set()

    def test_unicode_names(self):
        path = work_path('trl-unicode')
        log_w = TimeRotatingLog(path)
        goal_name = u'Goo\xf6aa\xe1llll!!!'
        rec = GoalRecord(name=goal_name,
                         value='',
                         value_type='',
                         value_format='')
        log_w.write(rec.to_list())

        log_r = TimeRotatingLog(path)
        records = list(log_r.process(stay_alive=False))
        self.assertEqual(len(records), 1)
        rec = Record.from_list(records[0][0])
        self.assertEqual(rec.name, goal_name)

    def test_resume(self):
        path = work_path('trl-resume')
        log_w = TimeRotatingLog(path)

        # Create a thread consumer
        log_r1 = TimeRotatingLog(path)

        consumed, consumer, ptr_container = make_thread_consumer(log_r1)

        try:
            # Write one record
            log_w.write(PageRecord(url='/herp').to_list())
            time.sleep(log_r1.sleep_delay * 10)
            # Check that one record was read.
            self.assertEqual(len(consumed), 1)
            self.assertEqual(consumed[0].url, '/herp')
        finally:
            # Kill the thread
            log_r1.killed.set()
            # Wait for it to die.
            time.sleep(log_r1.sleep_delay * 10)

        last_pointer = ptr_container[0]
        self.assertIsNotNone(last_pointer)

        try:
            # Write one record
            log_w.write(PageRecord(url='/derp').to_list())
            time.sleep(log_r1.sleep_delay * 10)
            # Create a new thread consumer
            log_r2 = TimeRotatingLog(path)
            consumed, consumer, _ = \
                make_thread_consumer(log_r2, process_from=last_pointer)
            time.sleep(log_r2.sleep_delay * 10)
            # Check that the second record was read.
            self.assertEqual(len(consumed), 1)
            self.assertEqual(consumed[0].url, '/derp')
        finally:
            log_r2.killed.set()
