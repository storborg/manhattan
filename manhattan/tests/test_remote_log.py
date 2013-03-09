import json
import os
import time
from threading import Thread
from unittest import TestCase

from manhattan.log.remote import RemoteLog, RemoteLogServer
from manhattan.log.timerotating import TimeRotatingLog


REDIS_KEY = 'manhattan:testing:log:queue'


class ServerThread(Thread):

    def __init__(self, log_server):
        self.log_server = log_server
        super(ServerThread, self).__init__()

    def run(self):
        self.log_server.run()


class TestRemoteLog(TestCase):

    @classmethod
    def setUpClass(cls):
        cls.text_log = TimeRotatingLog('/tmp/manhattan-tests/remote.log')
        cls.log = RemoteLog(key=REDIS_KEY)
        cls.log_server = RemoteLogServer(cls.text_log, key=REDIS_KEY)
        cls.log_server_thread = ServerThread(cls.log_server)
        for f in cls._get_log_files():
            os.remove(f)
        cls.log.db.delete(REDIS_KEY)

    @classmethod
    def tearDownClass(cls):
        cls.log_server.stop()
        cls.log_server.db.rpush(REDIS_KEY, '[]')  # HACK to break server loop

    @classmethod
    def _get_log_files(cls):
        files = []
        dirname = os.path.dirname(cls.text_log.path)
        for f in os.listdir(dirname):
            if f.startswith('remote.log'):
                files.append(os.path.join(dirname, f))
        return files

    def test_01_enqueue(self):
        record = ['a', 'b', 'c']
        self.log.write(record)
        item = self.log.db.blpop(REDIS_KEY)[1]
        stored_record = json.loads(item)[0]
        self.assertEqual(stored_record, record)

    def test_02_consume(self):
        self.log.write(['a', 'b', 'c'])
        self.log.write(['x', 'y', 'z'])
        self.log_server_thread.start()
        self.log.write(['1', '2', '3'])
        time.sleep(0.5)
        log_file = self._get_log_files()[0]
        with open(log_file) as fp:
            lines = fp.readlines()
            self.assertEqual(len(lines), 3)
        self.assertEqual(self.log.db.llen(REDIS_KEY), 0)
