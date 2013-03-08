import logging

from collections import deque

log = logging.getLogger(__name__)


class MemoryLog(object):
    """
    An in-memory log, intended for testing only. This log does not support
    resuming (server crash recovery).
    """
    def __init__(self):
        self.q = deque()

    def write(self, *records):
        log.info('Writing records: %r', records)
        self.q.extend(records)

    def process(self):
        log.info('Swapping out log.')
        to_process = self.q
        log.info('Creating new log.')
        self.q = deque()
        log.info('Playing back old log.')
        for record in to_process:
            log.info('Playing record: %r', record)
            yield record, None

    def purge(self):
        log.info('Purging log.')
        self.q = deque()
