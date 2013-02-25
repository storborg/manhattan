import logging
import logging.config
import sys
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
default_bind = 'tcp://127.0.0.1:5555'


class Server(Thread):

    def __init__(self, backend, bind=default_bind):
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
    return set(unicode(name).strip() for name in names.split(','))


def parse_complex_goals(complex):
    complex = complex or []
    configured = []
    for line in complex:
        complex_name, include_names, exclude_names = line.strip('"').split('|')
        include = parse_names(include_names)
        exclude = parse_names(exclude_names)
        configured.append((unicode(complex_name), include, exclude))
    return configured


def load_args_config(args):
    return dict(verbose=args.verbose,
                error_log_path=args.error_log_path,
                complex_goals=parse_complex_goals(args.complex),
                sqlalchemy_url=args.url,
                input_log_path=args.input_log_path,
                bind=args.bind)


def load_python_config(namespace):
    mod_name, attr_name = namespace.rsplit('.', 1)
    __import__(mod_name)
    mod = sys.modules[mod_name]
    return getattr(mod, attr_name)


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
    p.add_argument('--bind', type=str,
                   default='tcp://127.0.0.1:5555',
                   help='ZeroMQ socket description to bind to')

    p.add_argument('--config', type=str,
                   help='Python namespace to use for configuration')

    args = p.parse_args()

    if args.config:
        config = load_python_config(args.config)
    else:
        config = load_args_config(args)

    logging.config.dictConfig(logging_config(config.pop('verbose'),
                                             config.pop('error_log_path')))

    input_log_path = config.pop('input_log_path')
    bind = config.pop('bind')
    backend = Backend(**config)
    manhattan.server_backend = backend

    mhlog = TimeRotatingLog(input_log_path)
    worker = Worker(mhlog, backend, stats_every=5000)

    server = Server(backend, bind=bind)
    server.start()

    try:
        worker.run(stay_alive=True, killed_event=killed_event)
    finally:
        server.kill()
