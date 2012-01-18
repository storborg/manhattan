import zmq

from .text import TextLog


class ZeroMQLog(TextLog):
    def __init__(self, ctx, mode='w', endpoints='tcp://localhost:8128',
                 stay_alive=True):
        self.ctx = ctx
        self.stay_alive = stay_alive
        if mode == 'w':
            self.sock = ctx.socket(zmq.PUB)
            self.sock.connect(endpoints)
        else:
            self.sock = ctx.socket(zmq.SUB)
            self.sock.bind(endpoints)
            self.sock.setsockopt(zmq.SUBSCRIBE, '')

    def write(self, elements):
        self.sock.send(self.format(elements), flags=0)

    def process(self):
        flags = 0 if self.stay_alive else zmq.NOBLOCK
        while True:
            try:
                msg = self.parse(self.sock.recv(flags=flags))
            except zmq.ZMQError:
                break
            yield msg
