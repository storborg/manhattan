import time

from .util import choose_population
from .log import EventLog


class Visitor(object):
    """
    A handle to perform operations on the given visitor session.
    """
    def __init__(self, id, log):
        self.id = id
        self.log = log

    def timestamp(self):
        return str(int(time.time()))

    def page(self, request):
        self.log.write(['page', self.timestamp(), self.id, request.url])

    def pixel(self):
        self.log.write(['pixel', self.timestamp(), self.id])

    def goal(self, name, value=None):
        self.log.write(['goal', self.timestamp(), self.id, name, value])

    def split(self, test_name, populations=None):
        selected = choose_population(self.id + test_name, populations)
        self.log.write(['split', self.timestamp(), self.id, test_name,
                        selected])
        return selected
