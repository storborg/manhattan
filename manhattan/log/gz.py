import logging

from gzlog import GZLog

from .text import TextLog

log = logging.getLogger(__name__)


class GZEventLog(TextLog):

    def __init__(self, path):
        log.info('Creating GZEventLog at %r', path)
        self.writer = GZLog(path)

    def write(self, elements):
        log.info('Writing record: %r', elements)
        self.writer.write(self.format(elements))

    def process(self):
        log.info('Swapping out log.')
        self.writer.rotate()
        log.info('Playing back old log.')
        for record in self.writer.read():
            log.info('Playing record: %r', record)
            yield self.parse(record)
