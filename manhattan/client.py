import zmq
import code

ctx = zmq.Context()


class ServerError(Exception):
    pass


class Client(object):

    def __init__(self, connect='tcp://127.0.0.1:5555'):
        self.socket = ctx.socket(zmq.REQ)
        self.socket.connect(connect)

    def __getattr__(self, name):
        def rpc_method(*args, **kwargs):
            req = [name, args, kwargs]
            self.socket.send_json(req)
            status, resp = self.socket.recv_json()
            if status == 'ok':
                return resp
            else:
                raise ServerError(resp)
        return rpc_method


def main():
    client = Client()
    code.interact("The 'client' object is available for queries.",
                  local=dict(client=client))
