import logging

from .record import Record

log = logging.getLogger(__name__)


class Worker(object):

    def __init__(self, log, *backends):
        self.log = log
        self.backends = backends

    def handle_record(self, record):
        func_name = 'record_%s' % record.key
        for backend in self.backends:
            f = getattr(backend, func_name)
            f(*record.to_list()[1:])

    def run(self):
        log.info('Worker started processing.')
        for vals in self.log.process():
            log.info('Handling record %r', vals)
            record = Record.from_list(vals)
            self.handle_record(record)
        log.info('Worker finished processing.')
