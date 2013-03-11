import json
import os
import time
from threading import Thread
from unittest import TestCase

from manhattan.log.remote import make_redis, server, RemoteLog, RemoteLogServer
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
        for f in cls._get_log_files():
            os.remove(f)
        cls.log.db.delete(REDIS_KEY)

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
        item = self.log.db.lpop(REDIS_KEY)
        stored_record = json.loads(item)[0]
        self.assertEqual(stored_record, record)
        self.assertEqual(self.log.db.llen(REDIS_KEY), 0)

    def test_02_consume(self):
        log_server = RemoteLogServer(self.text_log, key=REDIS_KEY)
        log_server_thread = ServerThread(log_server)
        self.log.write(['a', 'b', 'c'])
        self.log.write(['x', 'y', 'z'])
        log_server_thread.start()
        self.log.write(['1', '2', '3'])
        time.sleep(0.5)
        log_file = self._get_log_files()[0]
        with open(log_file) as fp:
            lines = fp.readlines()
            self.assertEqual(len(lines), 3)
        self.assertEqual(self.log.db.llen(REDIS_KEY), 0)
        self.log.send_command('STOP')

    def test_server_script(self):
        """Create and immediately stop server."""
        self.log.send_command('STOP')
        server(['--key', REDIS_KEY])
        self.assertEqual(self.log.db.llen(REDIS_KEY), 0)

    def test_make_redis(self):
        redis = make_redis({'socket_timeout': 1}, socket_timeout=2)
        connection = redis.connection_pool.make_connection()
        self.assertEqual(connection.socket_timeout, 1)
        redis.rpush(REDIS_KEY, 'a')
        self.assertEqual(redis.lpop(REDIS_KEY), 'a')
        self.assertEqual(redis.llen(REDIS_KEY), 0)
