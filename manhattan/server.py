import logging
import logging.config
import sys
import argparse

from gevent import monkey, Greenlet
monkey.patch_all()

from gevent_zeromq import zmq

import manhattan
from manhattan.worker import Worker
from manhattan.log.timerotating import TimeRotatingLog
from manhattan.backend import Backend

log = logging.getLogger(__name__)


ctx = zmq.Context()


class Server(Greenlet):

    def __init__(self, backend, bind='tcp://127.0.0.1:5555'):
        Greenlet.__init__(self)
        self.backend = backend
        self.bind = bind

    def _run(self):
        s = ctx.socket(zmq.REP)
        s.bind(self.bind)
        while True:
            self.handle_client(s)

    def handle_client(self, sock):
        req = sock.recv_json()
        try:
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


def logging_config(verbose=False, filename=None):
    handlers = {
        'console': {
            'class': 'logging.StreamHandler',
            'stream': sys.stderr,
            'formatter': 'generic',
            'level': logging.DEBUG if verbose else logging.WARN,
        },
        'null': {
            'class': 'logging.NullHandler',
        }
    }

    if filename:
        handlers['root_file'] = {
            'class': 'logging.FileHandler',
            'formatter': 'generic',
            'level': 'NOTSET',
            'filename': filename,
        }

    return {
        'version': 1,
        'formatters': {
            'generic': {
                'format':
                "%(asctime)s %(levelname)-5.5s [%(name)s] %(message)s"
            },
        },
        'handlers': handlers,
        'loggers': {
            'manhattan': {
                'propagate': True,
                'level': 'NOTSET',
                'handlers': handlers.keys(),
            },
        },
        'root': {
            'level': 'DEBUG',
            'handlers': ['null']
        }
    }


def parse_names(names):
    return set(name.decode('ascii').strip() for name in names.split(','))


def parse_complex_goals(complex):
    complex = complex or []
    configured = []
    for line in complex:
        complex_name, include_names, exclude_names = line.strip('"').split('|')
        include = parse_names(include_names)
        exclude = parse_names(exclude_names)
        configured.append((complex_name.decode('ascii'), include, exclude))
    return configured


def main(killed_event=None):
    p = argparse.ArgumentParser(
        description='Run a Manhattan worker with a TimeRotatingLog.')

    p.add_argument('-v', '--verbose', dest='verbose', action='store_true',
                   default=False, help='Print detailed output')
    p.add_argument('-p', '--path', dest='input_log_path', type=str,
                   help='Input Manhattan log path')
    p.add_argument('--log', dest='error_log_path', type=str,
                   help='Path to error/debug log')
    p.add_argument('-u', '--url', dest='url', type=str,
                   help='SQL backend URL')
    p.add_argument('-c', '--complex', dest='complex', action='append',
                   help='Configure complex goal, like '
                   'name|include a, include b|exclude a')

    args = p.parse_args()

    logging.config.dictConfig(logging_config(args.verbose,
                                             args.error_log_path))

    complex_goals = parse_complex_goals(args.complex)

    backend = Backend(sqlalchemy_url=args.url, complex_goals=complex_goals)
    manhattan.server_backend = backend

    mhlog = TimeRotatingLog(args.input_log_path)
    worker = Worker(mhlog, backend, stats_every=5000)

    server = Server(backend)
    server.start()

    try:
        worker.run(stay_alive=True, killed_event=killed_event)
    finally:
        server.kill()
