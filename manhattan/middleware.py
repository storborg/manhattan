from webob import Request, Response
from itsdangerous import Signer

from .visitor import Visitor
from .util import nonce, transparent_pixel, pixel_tag


class ManhattanMiddleware(object):

    def __init__(self, app, log, secret_key='blah', cookie_name='vis',
                 pixel_path='/vpixel.gif'):
        self.app = app
        self.cookie_name = cookie_name
        self.log = log
        self.signer = Signer(secret_key)
        self.pixel_path = pixel_path

    def inject_pixel(self, resp):
        tag = pixel_tag(self.pixel_path)

        def wrap_iter(orig_iter):
            for chunk in orig_iter:
                yield chunk.replace('</body>', '%s</body>' % tag)

        resp.app_iter = wrap_iter(resp.app_iter)
        resp.content_length = None

    def handle_pixel(self, visitor, fresh):
        if not fresh:
            visitor.pixel()
        resp = Response(transparent_pixel)
        resp.content_type = 'image/gif'
        return resp

    def __call__(self, environ, start_response):
        req = Request(environ)

        if self.cookie_name in req.cookies:
            vid = self.signer.unsign(req.cookies[self.cookie_name])
            fresh = False
        else:
            vid = nonce()
            fresh = True

        req.environ['manhattan.visitor'] = visitor = Visitor(vid, self.log)

        if self.pixel_path and req.path_info == self.pixel_path:
            resp = self.handle_pixel(visitor, fresh)
            return resp(environ, start_response)

        resp = req.get_response(self.app)
        visitor.page(req)

        if fresh:
            resp.set_cookie(self.cookie_name, self.signer.sign(visitor.id))

        if self.pixel_path and resp.content_type == 'text/html':
            self.inject_pixel(resp)

        return resp(environ, start_response)
