import argparse
from threading import Thread

import zmq

from manhattan.log.timerotating import TimeRotatingLog


context = zmq.Context()


class RemoteLog(object):

    """Sends log entries to a remote server."""

    def __init__(self, connect='tcp://localhost:5556'):
        self.socket = context.socket(zmq.PUSH)
        self.socket.connect(connect)

    def write(self, elements):
        """Send ``elements`` to remote logger."""
        self.socket.send_json(elements)


class RemoteLogServer(object):

    def __init__(self, log, bind='tcp://*:5556'):
        self.bind = bind
        self.log = log

    def run(self):
        context = zmq.Context()
        socket = context.socket(zmq.PULL)
        socket.bind('tcp://*:5556')
        while True:
            log_entry = socket.recv_json()
            self.log.write(log_entry)


def server(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('-b', '--bind', default='tcp://*:5556')
    parser.add_argument('-p', '--path', default='log/manhattan.log')
    args = parser.parse_args(argv) if argv else parser.parse_args()
    log_server = RemoteLogServer(TimeRotatingLog(args.path), bind=args.bind)
    log_server.run()
