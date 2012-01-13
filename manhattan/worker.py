import logging

log = logging.getLogger(__name__)


class Worker(object):

    def __init__(self, log, *backends):
        self.log = log
        self.backends = backends

    def handle_record(self, record):
        func_name = 'record_%s' % record[0]
        for backend in self.backends:
            f = getattr(backend, func_name)
            f(*record[1:])

    def run(self):
        log.info('Worker started processing.')
        for record in self.log.process():
            log.info('Handling record %r', record)
            self.handle_record(record)
        log.info('Worker finished processing.')
