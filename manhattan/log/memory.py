import logging

from collections import deque

log = logging.getLogger(__name__)


class MemoryLog(object):
    def __init__(self):
        self.q = deque()

    def write(self, elements):
        log.info('Writing record: %r', elements)
        self.q.append(elements)

    def process(self):
        log.info('Swapping out log.')
        to_process = self.q
        log.info('Creating new log.')
        self.q = deque()
        log.info('Playing back old log.')
        for record in to_process:
            log.info('Playing record: %r', record)
            yield record

    def purge(self):
        log.info('Purging log.')
        self.q = deque()
