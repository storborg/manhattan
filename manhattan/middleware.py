from webob import Request
from itsdangerous import Signer

from .visitor import Visitor
from .util import nonce


class ManhattanMiddleware(object):

    def __init__(self, app, log, secret_key='blah', cookie_name='vis'):
        self.app = app
        self.cookie_name = cookie_name
        self.log = log
        self.signer = Signer(secret_key)

    def __call__(self, environ, start_response):
        req = Request(environ)
        if self.cookie_name in req.cookies:
            vid = self.signer.unsign(req.cookies[self.cookie_name])
            fresh = False
        else:
            vid = nonce()
            fresh = True

        req.environ['manhattan.visitor'] = visitor = Visitor(vid, self.log)

        visitor.page(req)
        resp = req.get_response(self.app)

        if fresh:
            resp.set_cookie(self.cookie_name, self.signer.sign(visitor.id))

        return resp(environ, start_response)
