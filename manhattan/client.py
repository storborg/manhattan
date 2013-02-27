import argparse
import zmq
import code

default_bind = 'tcp://127.0.0.1:5555'


ctx = zmq.Context()


class ServerError(Exception):
    pass


class TimeoutError(Exception):
    pass


class Client(object):

    def __init__(self, connect=default_bind, wait=3000):
        self.sock = ctx.socket(zmq.REQ)
        self.sock.setsockopt(zmq.LINGER, 0)
        self.sock.connect(connect)

        self.poller = zmq.Poller()
        self.poller.register(self.sock, zmq.POLLIN)

        self.wait = wait

    def __getattr__(self, name):
        def rpc_method(*args, **kwargs):
            req = [name, args, kwargs]
            self.sock.send_json(req)

            if self.poller.poll(self.wait):
                status, resp = self.sock.recv_json()
                if status == 'ok':
                    return resp
                else:
                    raise ServerError(resp)
            else:
                raise TimeoutError('Timed out after %d ms waiting for reply' %
                                   self.wait)
        return rpc_method


def main():
    p = argparse.ArgumentParser(description='Run a Manhattan client.')
    p.add_argument('--connect', type=str,
                   default=default_bind,
                   help='ZeroMQ socket description to connect to')
    args = p.parse_args()

    client = Client(args.connect)
    code.interact("The 'client' object is available for queries.",
                  local=dict(client=client))
