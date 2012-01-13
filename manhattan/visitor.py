import logging

import time

from .util import choose_population

log = logging.getLogger(__name__)


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
        log.debug('page: %s %s', self.id, request.url)
        self.log.write(['page', self.timestamp(), self.id, request.url])

    def pixel(self):
        log.debug('pixel: %s', self.id)
        self.log.write(['pixel', self.timestamp(), self.id])

    def goal(self, name, value=None):
        log.debug('goal: %s %s', self.id, name)
        self.log.write(['goal', self.timestamp(), self.id, name, str(value)])

    def split(self, test_name, populations=None):
        log.debug('split: %s %s', self.id, test_name)
        selected = choose_population(self.id + test_name, populations)
        self.log.write(['split', self.timestamp(), self.id, test_name,
                        selected])
        return selected
