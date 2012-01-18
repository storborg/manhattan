import logging

from gzlog import GZLog

log = logging.getLogger(__name__)


class GZEventLog(object):

    def __init__(self, path):
        log.info('Creating GZEventLog at %r', path)
        self.writer = GZLog(path)

    def format(self, elements):
        return '\t'.join(el.encode('string_escape') for el in elements)

    def parse(self, record):
        return [el.decode('string_escape') for el in record.split('\t')]

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
