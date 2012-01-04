import logging

from .util import choose_population

log = logging.getLogger(__name__)


class Visitor(object):
    """
    A handle to perform operations on the given visitor session.
    """

    def __init__(self, id, backend):
        self.id = id
        self.backend = backend

    def pageview(self, request):
        log.debug('pageview: %s - %s', self.id, request.url)

    def pixel(self):
        log.debug('pixel: %s', self.id)

    def goal(self, name):
        log.debug('goal: %s - %s', self.id, name)

    def split(self, test_name, populations=None):
        selected = choose_population(self.id + test_name, populations)
        log.debug('split: %s - %s -> %s', self.id,
                  test_name, selected)
        return selected
