import logging
import argparse
from threading import Thread

import zmq
from zmq.eventloop import ioloop

import manhattan
from manhattan.worker import Worker
from manhattan.log.timerotating import TimeRotatingLog
from manhattan.backend import Backend

log = logging.getLogger(__name__)


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
            req = sock.recv_json()
            resp = self.handle(req)
            msg = ['ok', resp]
        except Exception as e:
            msg = ['error', '%s: %s' % (e.__class__.__name__, str(e))]
        sock.send_json(msg)

    def handle(self, req):
        log.info('Handling request: %r', req)
        method, args, kwargs = req
        resp = getattr(self.backend, method)(*args, **kwargs)
        log.info('Returning response: %r', resp)
        return resp


def main(killed_event=None):
    p = argparse.ArgumentParser(description='Run a Manhattan worker with a '
                                'TimeRotatingLog and memory backend.')
    p.add_argument('-v', '--verbose', dest='loglevel', action='store_const',
                   const=logging.DEBUG, default=logging.WARN,
                   help='Print detailed output')
    p.add_argument('-p', '--path', dest='log_path', type=str,
                   help='Log path')
    p.add_argument('-u', '--url', dest='url', type=str,
                   help='SQL backend URL')

    args = p.parse_args()

    logging.basicConfig(level=args.loglevel)

    backend = Backend(sqlalchemy_url=args.url)
    manhattan.server_backend = backend

    log = TimeRotatingLog(args.log_path)
    worker = Worker(log, backend, stats_every=5000)

    server = Server(backend)
    server.start()

    try:
        worker.run(stay_alive=True, killed_event=killed_event)
    finally:
        server.kill()
