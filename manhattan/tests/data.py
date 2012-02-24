import types
from random import randint

from webob import Request

from manhattan import visitor
from manhattan.visitor import Visitor


test_clickstream = [
    (10, 'page', 'a', '/'),
    (10, 'page', 'b', '/cheese'),
    (132, 'pixel', 'a'),
    (240, 'page', 'a', '/potatoes'),
    (290, 'page', 'a', '/potatoes/sweet'),
    (295, 'page', 'bot', '/potatoes/russet'),
    (300, 'pixel', 'b'),
    (382, 'page', 'a', '/potatoes/russet'),
    (385, 'goal', 'a', 'add to cart', ''),
    (394, 'page', 'b', '/cheese/parmesan'),
    (448, 'page', 'c', '/fruit/apples'),
    (462, 'page', 'b', '/cart'),
    (749, 'goal', 'f', 'fake goal', ''),
    (1120, 'goal', 'b', 'add to cart', ''),
    (1180, 'page', 'bot', '/potatoes/russet'),
    (1200, 'page', 'f', '/'),
    (1202, 'pixel', 'f'),
    (1596, 'page', 'c', '/fruit/bananas'),
    (1602, 'split', 'b', 'red checkout form'),
    (1602, 'page', 'b', '/checkout'),
    (1602, 'goal', 'b', 'began checkout', ''),
    (1706, 'page', 'f', '/fruit'),
    (1821, 'page', 'bot', 'fruit'),
    (1920, 'page', 'bot', '/cart'),
    (1950, 'goal', 'bot', 'add to cart', ''),
    (1996, 'page', 'a', '/cart'),
    (1996, 'goal', 'a', 'add to cart', ''),
    (2112, 'pixel', 'c'),
    (2196, 'page', 'a', '/cheese/gouda'),
    (2356, 'page', 'a', '/cheese'),
    (2680, 'page', 'b', '/cheese'),
    (2840, 'page', 'd', '/'),
    (2846, 'pixel', 'd'),
    (3110, 'split', 'b', 'red checkout form'),
    (3340, 'page', 'd', '/cheese'),
    (3514, 'page', 'd', '/cheese/gruyere'),
    (3514, 'page', 'b', '/checkout/complete'),
    (3514, 'goal', 'b', 'completed checkout', 31.78),
    (3514, 'goal', 'b', 'order margin', 22.5),
    (3514, 'goal', 'b', 'margin per session', 7.15),
    (3600, 'page', 'c', '/'),
    (3620, 'page', 'd', '/cart'),
    (4114, 'goal', 'd', 'add to cart', ''),
    (4278, 'split', 'd', 'red checkout form'),
    (4278, 'page', 'd', '/checkout'),
    (4278, 'goal', 'd', 'began checkout', ''),
    (4534, 'page', 'a', '/account'),
    (4600, 'page', 'e', '/fruit'),
    (4616, 'pixel', 'e'),
    (4700, 'page', 'bot', '/fruit/cherries'),
    (4990, 'split', 'd', 'red checkout form'),
    (4990, 'page', 'd', '/checkout/complete'),
    (4990, 'goal', 'd', 'completed checkout', 64.99),
    (4990, 'goal', 'd', 'order margin', 20.1),
    (4990, 'goal', 'd', 'margin per session', 13.06),
    (5002, 'page', 'e', '/fruit/pineapples'),
    (5174, 'page', 'e', '/fruit/cherries'),
    (5226, 'page', 'e', '/fruit/pears'),
    (5244, 'page', 'e', '/cart'),
    (5244, 'goal', 'e', 'add to cart', ''),
    (5950, 'split', 'e', 'red checkout form'),
    (5950, 'page', 'e', '/checkout'),
    (5950, 'goal', 'e', 'began checkout', ''),
    (6278, 'page', 'd', '/account'),
    (6396, 'page', 'd', '/'),
    (6620, 'split', 'e', 'red checkout form'),
    (6620, 'page', 'e', '/checkout/complete'),
    (6620, 'goal', 'e', 'completed checkout', 11.42),
    (6620, 'goal', 'e', 'order margin', 27.8),
    (6620, 'goal', 'e', 'margin per session', 3.17),
    (6988, 'page', 'b', '/fruit'),
    (7020, 'page', 'f', '/cheese'),
    (7042, 'page', 'f', '/cheese/cheddar'),
    (7068, 'page', 'f', '/cart'),
    (7068, 'goal', 'f', 'add to cart', ''),
    (7198, 'page', 'f', '/cheese'),
    (7246, 'split', 'f', 'red checkout form'),
    (7246, 'page', 'f', '/checkout'),
    (7246, 'goal', 'f', 'began checkout', '')
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
    }

    visitors = {}

    def get_visitor(vid):
        if vid not in visitors:
            visitors[vid] = Visitor(vid, log)
        return visitors[vid]

    def set_fake_timestamp(v, ts):
        def fake_timestamp(self):
            return '%d.%04d' % (ts, randint(0, 9999))
        v.timestamp = types.MethodType(fake_timestamp, v, Visitor)

    stream = test_clickstream
    if first and last:
        stream = stream[first:last]
    elif first:
        stream = stream[first:]
    elif last:
        stream = stream[:last]

    for action in stream:
        ts = action[0]
        cmd = action[1]
        v = get_visitor(action[2])
        args = action[3:]

        set_fake_timestamp(v, ts)

        if cmd == 'page':
            req = Request.blank(args[0])
            req.user_agent = browsers[action[2]]
            v.page(req)
        elif cmd == 'pixel':
            v.pixel()
        elif cmd == 'goal':
            goal_name = args[0].decode('ascii')
            value = args[1]
            value_type = value_types.get(goal_name)
            value_format = value_formats.get(goal_name)
            v.goal(goal_name, value=value,
                   value_type=value_type,
                   value_format=value_format)
        elif cmd == 'split':
            v.split(args[0].decode('ascii'))
