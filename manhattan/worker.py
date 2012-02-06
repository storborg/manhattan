import logging

from .record import Record

log = logging.getLogger(__name__)


class Worker(object):

    def __init__(self, log, *backends):
        self.log = log
        self.backends = backends

    def run(self):
        log.info('Worker started processing.')
        for vals in self.log.process():
            log.info('Handling record %r', vals)
            record = Record.from_list(vals)
            for backend in self.backends:
                backend.handle(record)
        log.info('Worker finished processing.')
