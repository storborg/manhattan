import logging
import argparse
import json
from threading import Thread

import zmq
from zmq.eventloop import ioloop

from manhattan.worker import Worker
from manhattan.log.timerotating import TimeRotatingLog
from manhattan.backends.memory import MemoryBackend


loop = ioloop.IOLoop.instance()
ctx = zmq.Context()


class Server(Thread):

    def __init__(self, backend, bind='tcp://127.0.0.1:5555'):
        Thread.__init__(self)
        self.backend = backend
        self.bind = bind

    def run(self):
        s = ctx.socket(zmq.REP)
        s.bind(self.bind)
        loop.add_handler(s, self.handle_zmq, zmq.POLLIN)
        loop.start()

    def kill(self):
        loop.stop()

    def handle_zmq(self, sock, events):
        try:
            req = json.loads(sock.recv())
            resp = self.handle(req)
            msg = json.dumps(['ok', resp])
        except Exception as e:
            msg = json.dumps(['error',
                              '%s: %s' % (e.__class__.__name__, str(e))])
        sock.send(msg)

    def handle(self, req):
        method, args, kwargs = req
        return getattr(self.backend, method)(*args, **kwargs)


def main(killed_event=None):
    p = argparse.ArgumentParser(description='Run a Manhattan worker with a '
                                'TimeRotatingLog and memory backend.')
    p.add_argument('-v', '--verbose', dest='loglevel', action='store_const',
                   const=logging.DEBUG, default=logging.WARN,
                   help='Print detailed output')
    p.add_argument('-p', '--path', dest='log_path', type=str,
                   help='Log path')
    p.add_argument('--stay-alive', dest='stay_alive', action='store_true',
                   help='Stay alive and continue processing')

    args = p.parse_args()

    logging.basicConfig(level=args.loglevel)

    backend = MemoryBackend()
    log = TimeRotatingLog(args.log_path)
    worker = Worker(log, backend, stats_every=5000)

    server = Server(backend)
    server.start()

    try:
        worker.run(stay_alive=True, killed_event=killed_event)
    finally:
        server.kill()
