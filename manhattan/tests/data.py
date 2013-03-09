import types
from random import randint

from webob import Request

from manhattan import visitor
from manhattan.visitor import Visitor

from .base import work_path


test_complex_goals = [
    (u'abandoned cart', set([u'add to cart']), set([u'began checkout'])),
    (u'abandoned checkout',
     set([u'began checkout']), set([u'completed checkout'])),
    (u'abandoned after validation failure',
     set([u'began checkout', u'checkout validation failed']),
     set([u'completed checkout'])),
    (u'abandoned after payment failure',
     set([u'began checkout', u'payment failed']),
     set([u'completed checkout'])),
]


sampleconfig = {
    'input_log_path': work_path('clientserver-python-config'),
    'sqlalchemy_url': 'sqlite:///' + work_path('sampleconfig.db'),
    'complex_goals': test_complex_goals,
    'bind': 'tcp://127.0.0.1:5556',
    'verbose': False,
    'error_log_path': work_path('python-config-debug.log'),
}


test_clickstream = [
    (1, 10, 'page', 'a', '/'),
    (1, 10, 'page', 'b', '/cheese'),
    (1, 132, 'pixel', 'a'),
    (1, 240, 'page', 'a', '/potatoes'),
    (1, 290, 'page', 'a', '/potatoes/sweet'),
    (1, 295, 'page', 'bot', '/potatoes/russet'),
    (1, 300, 'pixel', 'b'),
    (1, 382, 'page', 'a', '/potatoes/russet'),
    (1, 385, 'goal', 'a', 'add to cart', ''),
    (1, 394, 'page', 'b', '/cheese/parmesan'),
    (2, 401, 'page', 'q', '/'),
    (1, 448, 'page', 'c', '/fruit/apples'),
    (1, 462, 'page', 'b', '/cart'),
    (2, 544, 'page', 'q', '/candy'),
    (2, 545, 'pixel', 'q'),
    (2, 680, 'page', 'q', '/candy'),
    (1, 749, 'goal', 'f', 'fake goal', ''),
    (1, 1120, 'goal', 'b', 'add to cart', ''),
    (1, 1180, 'page', 'bot', '/potatoes/russet'),
    (1, 1200, 'page', 'f', '/'),
    (1, 1202, 'pixel', 'f'),
    (2, 1311, 'page', 'q', '/candy/molds'),
    (1, 1596, 'page', 'c', '/fruit/bananas'),
    (1, 1602, 'split', 'b', 'red checkout form'),
    (1, 1602, 'page', 'b', '/checkout'),
    (1, 1602, 'goal', 'b', 'began checkout', ''),
    (1, 1706, 'page', 'f', '/fruit'),
    (2, 1807, 'page', 'q', '/cart'),
    (2, 1807, 'goal', 'q', 'add to cart', ''),
    (1, 1821, 'page', 'bot', 'fruit'),
    (1, 1920, 'page', 'bot', '/cart'),
    (1, 1950, 'goal', 'bot', 'add to cart', ''),
    (1, 1996, 'page', 'a', '/cart'),
    (1, 1996, 'goal', 'a', 'add to cart', ''),
    (1, 2043, 'goal', 'b', 'checkout validation failed', ''),
    (1, 2112, 'pixel', 'c'),
    (1, 2196, 'page', 'a', '/cheese/gouda'),
    (1, 2356, 'page', 'a', '/cheese'),
    (2, 2477, 'page', 'q', '/checkout'),
    (2, 2477, 'goal', 'q', 'began checkout', ''),
    (1, 2680, 'page', 'b', '/cheese'),
    (1, 2840, 'page', 'd', '/'),
    (1, 2846, 'pixel', 'd'),
    (1, 3110, 'split', 'b', 'red checkout form'),
    (1, 3340, 'page', 'd', '/cheese'),
    (1, 3514, 'page', 'd', '/cheese/gruyere'),
    (1, 3514, 'page', 'b', '/checkout/complete'),
    (1, 3514, 'goal', 'b', 'completed checkout', 31.78),
    (1, 3514, 'goal', 'b', 'order margin', 22.5),
    (1, 3514, 'goal', 'b', 'margin per session', 7.15),
    (1, 3600, 'page', 'c', '/'),
    (1, 3620, 'page', 'd', '/cart'),
    (1, 4114, 'goal', 'd', 'add to cart', ''),
    (1, 4278, 'split', 'd', 'red checkout form'),
    (1, 4278, 'page', 'd', '/checkout'),
    (1, 4278, 'goal', 'd', 'began checkout', ''),
    (1, 4534, 'page', 'a', '/account'),
    (1, 4600, 'page', 'e', '/fruit'),
    (1, 4616, 'pixel', 'e'),
    (1, 4700, 'page', 'bot', '/fruit/cherries'),
    (1, 4990, 'split', 'd', 'red checkout form'),
    (1, 4990, 'page', 'd', '/checkout/complete'),
    (1, 4990, 'goal', 'd', 'completed checkout', 64.99),
    (1, 4990, 'goal', 'd', 'order margin', 20.1),
    (1, 4990, 'goal', 'd', 'margin per session', 13.06),
    (1, 5002, 'page', 'e', '/fruit/pineapples'),
    (1, 5174, 'page', 'e', '/fruit/cherries'),
    (1, 5226, 'page', 'e', '/fruit/pears'),
    (1, 5244, 'page', 'e', '/cart'),
    (1, 5244, 'goal', 'e', 'add to cart', ''),
    (1, 5950, 'split', 'e', 'red checkout form'),
    (1, 5950, 'page', 'e', '/checkout'),
    (1, 5950, 'goal', 'e', 'began checkout', ''),
    (1, 6278, 'page', 'd', '/account'),
    (1, 6396, 'page', 'd', '/'),
    (1, 6620, 'split', 'e', 'red checkout form'),
    (1, 6620, 'page', 'e', '/checkout/complete'),
    (1, 6620, 'goal', 'e', 'completed checkout', 11.42),
    (1, 6620, 'goal', 'e', 'order margin', 27.8),
    (1, 6620, 'goal', 'e', 'margin per session', 3.17),
    (1, 6988, 'page', 'b', '/fruit'),
    (1, 7020, 'page', 'f', '/cheese'),
    (1, 7042, 'page', 'f', '/cheese/cheddar'),
    (1, 7068, 'page', 'f', '/cart'),
    (1, 7068, 'goal', 'f', 'add to cart', ''),
    (1, 7198, 'page', 'f', '/cheese'),
    (1, 7246, 'split', 'f', 'red checkout form'),
    (1, 7246, 'page', 'f', '/checkout'),
    (1, 7246, 'goal', 'f', 'began checkout', ''),
    (1, 7350, 'goal', 'f', 'payment failed', ''),
]


def run_clickstream(log, first=None, last=None):
    value_types = {
        'completed checkout': visitor.SUM,
        'order margin': visitor.AVERAGE,
        'margin per session': visitor.PER,
    }

    value_formats = {
        'completed checkout': visitor.CURRENCY,
        'order margin': visitor.PERCENTAGE,
        'margin per session': visitor.CURRENCY,
    }

    browsers = {
        'a': u'Chrome/666.0',
        'b': u'Safari/12345',
        'c': u'Firefox/infinity',
        'd': u'Chrome/17',
        'e': u'Opera/sucks',
        'f': u'MSIE/9',
        'bot': u'ScroogleBot',
        'q': u'Chrome/641.1',
    }

    visitors = {}

    def get_visitor(vid, site_id):
        if vid not in visitors:
            visitors[vid] = Visitor(
                vid, log, site_id=site_id, buffer_writes=False)
        return visitors[vid]

    def set_fake_timestamp(v, ts):
        def fake_timestamp(self):
            return '%d.%04d' % (ts, randint(0, 9999))
        v.timestamp = types.MethodType(fake_timestamp, v)

    stream = test_clickstream
    if first and last:
        stream = stream[first:last]
    elif first:
        stream = stream[first:]
    elif last:
        stream = stream[:last]

    for action in stream:
        site_id = action[0]
        ts = action[1]
        cmd = action[2]
        vid = action[3]
        v = get_visitor(vid, site_id)
        args = action[4:]

        set_fake_timestamp(v, ts)

        if cmd == 'page':
            req = Request.blank(args[0])
            req.user_agent = browsers[vid]
            v.page(req)
        elif cmd == 'pixel':
            v.pixel()
        elif cmd == 'goal':
            goal_name = unicode(args[0])
            value = args[1]
            value_type = value_types.get(goal_name)
            value_format = value_formats.get(goal_name)
            v.goal(goal_name, value=value,
                   value_type=value_type,
                   value_format=value_format)
        elif cmd == 'split':
            v.split(unicode(args[0]))
