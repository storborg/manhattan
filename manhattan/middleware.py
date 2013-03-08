from webob import Request, Response

from .visitor import Visitor
from .util import nonce, transparent_pixel, pixel_tag, Signer


class ManhattanMiddleware(object):

    def __init__(self, app, log, secret, cookie_name='manhattan',
                 pixel_path='/vpixel.gif', host_map=None, buffer_writes=True):
        self.app = app
        self.cookie_name = cookie_name
        self.log = log
        self.signer = Signer(secret)
        self.pixel_path = pixel_path
        self.host_map = host_map or {}
        self.buffer_writes = buffer_writes

    def inject_pixel(self, resp):
        tag = pixel_tag(self.pixel_path)

        def wrap_iter(orig_iter):
            for chunk in orig_iter:
                yield chunk.replace('</body>', '%s</body>' % tag)

        resp.app_iter = wrap_iter(resp.app_iter)

    def handle_pixel(self, visitor, fresh):
        if not fresh:
            visitor.pixel()
        resp = Response(transparent_pixel)
        resp.content_type = 'image/gif'
        return resp

    def count_page(self, req):
        return (req.method in ('GET', 'POST') and
                req.headers.get('X-Purpose') != 'preview')

    def get_visitor_id(self, req):
        signed_value = req.cookies[self.cookie_name]
        return self.signer.unsign(signed_value)

    def __call__(self, environ, start_response):
        req = Request(environ)

        if self.cookie_name in req.cookies:
            vid = self.get_visitor_id(req)
            fresh = False
        else:
            vid = nonce()
            fresh = True

        site_id = self.host_map.get(req.host.split(':', 1)[0], 0)

        req.environ['manhattan.visitor'] = visitor = Visitor(
            vid, self.log, site_id, self.buffer_writes)

        if self.pixel_path and req.path_info == self.pixel_path:
            resp = self.handle_pixel(visitor, fresh)
        else:
            resp = req.get_response(self.app)

            if self.count_page(req):
                visitor.page(req)

            if fresh:
                resp.set_cookie(self.cookie_name, self.signer.sign(visitor.id),
                                httponly=True)

            if self.pixel_path and resp.content_type == 'text/html':
                self.inject_pixel(resp)

        visitor.flush()
        return resp(environ, start_response)
