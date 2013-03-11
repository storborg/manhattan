import argparse
import json

import redis

from manhattan.log.timerotating import TimeRotatingLog


DEFAULT_REDIS_KEY = 'manhattan:log:queue'


def make_redis(kwargs=None, **defaults):
    if kwargs is None:
        kwargs = {}
    for k in defaults:
        if k not in kwargs:
            kwargs[k] = defaults[k]
    return redis.Redis(**kwargs)


class RemoteLog(object):

    """Sends log entries to a remote server."""

    def __init__(self, key=DEFAULT_REDIS_KEY, redis_kwargs=None):
        self.key = key
        self.db = make_redis(redis_kwargs, socket_timeout=1)

    def write(self, *records):
        """Send ``records`` to remote logger."""
        self.db.rpush(self.key, json.dumps(records))

    def send_command(self, command):
        self.db.rpush(self.key, command)


class RemoteLogServer(object):

    """Consumes log entries from a Redis queue and writes them to a log."""

    def __init__(self, log, key=DEFAULT_REDIS_KEY, redis_kwargs=None):
        self.log = log
        self.key = key
        self.db = make_redis(redis_kwargs)
        self.running = False

    def run(self):
        self.running = True
        while self.running:
            records = self.db.blpop(self.key)[1]
            if records == 'STOP':
                self.stop()
            else:
                records = json.loads(records)
                self.log.write(*records)

    def stop(self):
        self.running = False


def server(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--path', default='log/manhattan.log')
    parser.add_argument('-k', '--key', default=DEFAULT_REDIS_KEY)
    args = parser.parse_args(argv)
    log_server = RemoteLogServer(TimeRotatingLog(args.path), args.key)
    log_server.run()
