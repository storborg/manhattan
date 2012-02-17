import zmq
import json
import code

ctx = zmq.Context()


class Client(object):

    def __init__(self, connect='tcp://127.0.0.1:5555'):
        self.socket = ctx.socket(zmq.REQ)
        self.connect = connect
        self.socket.connect(self.connect)

    def __getattr__(self, name):
        def rpc_method(*args, **kwargs):
            req = [name, args, kwargs]
            self.socket.send(json.dumps(req))
            return json.loads(self.socket.recv())
        return rpc_method


def main():
    client = Client()
    code.interact("The 'client' object is available for queries.",
                  local=dict(client=client))
