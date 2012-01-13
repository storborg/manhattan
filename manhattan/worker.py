import logging

log = logging.getLogger(__name__)


class Worker(object):

    def __init__(self, log, backend):
        self.log = log
        self.backend = backend

    def handle_record(self, record):
        cmd = record[0]
        f = getattr(self.backend, 'record_%s' % cmd)
        f(*record[1:])

    def run(self):
        log.info('Worker started processing.')
        for record in self.log.process():
            log.info('Handling record %r', record)
            self.handle_record(record)
