from webob import Request

from manhattan import visitor
from manhattan.visitor import Visitor


test_clickstream = [
    ('page', 'a', '/'),
    ('page', 'b', '/cheese'),
    ('pixel', 'a'),
    ('page', 'a', '/potatoes'),
    ('page', 'a', '/potatoes/sweet'),
    ('pixel', 'b'),
    ('page', 'a', '/potatoes/russet'),
    ('page', 'b', '/cheese/parmesan'),
    ('page', 'c', '/fruit/apples'),
    ('page', 'b', '/cart'),
    ('goal', 'b', 'add to cart', ''),
    ('page', 'f', '/'),
    ('pixel', 'f'),
    ('page', 'c', '/fruit/bananas'),
    ('split', 'b', 'red checkout form'),
    ('page', 'b', '/checkout'),
    ('goal', 'b', 'began checkout', ''),
    ('page', 'f', '/fruit'),
    ('page', 'a', '/cart'),
    ('goal', 'a', 'add to cart', ''),
    ('pixel', 'c'),
    ('page', 'a', '/cheese/gouda'),
    ('page', 'a', '/cheese'),
    ('page', 'b', '/cheese'),
    ('page', 'd', '/'),
    ('pixel', 'd'),
    ('split', 'b', 'red checkout form'),
    ('page', 'd', '/cheese'),
    ('page', 'd', '/cheese/gruyere'),
    ('page', 'b', '/checkout/complete'),
    ('goal', 'b', 'completed checkout', 31.78),
    ('goal', 'b', 'order margin', 22.5),
    ('goal', 'b', 'margin per session', 7.15),
    ('page', 'c', '/'),
    ('page', 'd', '/cart'),
    ('goal', 'd', 'add to cart', ''),
    ('page', 'd', '/checkout'),
    ('goal', 'd', 'began checkout', ''),
    ('page', 'a', '/account'),
    ('page', 'e', '/fruit'),
    ('pixel', 'e'),
    ('page', 'd', '/checkout/complete'),
    ('goal', 'd', 'completed checkout', 64.99),
    ('goal', 'd', 'order margin', 20.1),
    ('goal', 'd', 'margin per session', 13.06),
    ('page', 'e', '/fruit/pineapples'),
    ('page', 'e', '/fruit/cherries'),
    ('page', 'e', '/fruit/pears'),
    ('page', 'e', '/cart'),
    ('goal', 'e', 'add to cart', ''),
    ('page', 'e', '/checkout'),
    ('goal', 'e', 'began checkout', ''),
    ('page', 'd', '/account'),
    ('page', 'd', '/'),
    ('page', 'e', '/checkout/complete'),
    ('goal', 'e', 'completed checkout', 11.42),
    ('goal', 'e', 'order margin', 27.8),
    ('goal', 'e', 'margin per session', 3.17),
    ('page', 'b', '/fruit'),
    ('page', 'f', '/cheese'),
    ('page', 'f', '/cheese/cheddar'),
    ('page', 'f', '/cart'),
    ('goal', 'f', 'add to cart', ''),
    ('page', 'f', '/cheese'),
    ('page', 'f', '/checkout'),
    ('goal', 'f', 'began checkout', ''),
]


def run_clickstream(log):
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

    visitors = {}

    def get_visitor(vid):
        if vid not in visitors:
            visitors[vid] = Visitor(vid, log)
        return visitors[vid]

    for action in test_clickstream:
        cmd = action[0]
        v = get_visitor(action[1])
        args = action[2:]

        if cmd == 'page':
            req = Request.blank(args[0])
            v.page(req)
        elif cmd == 'pixel':
            v.pixel()
        elif cmd == 'goal':
            goal_name = args[0]
            value = args[1]
            value_type = value_types.get(goal_name)
            value_format = value_formats.get(goal_name)
            v.goal(goal_name, value=value,
                   value_type=value_type,
                   value_format=value_format)
        elif cmd == 'split':
            v.split(args[0])
