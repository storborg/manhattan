import logging
import logging.config
import argparse

from .record import Record
from .backends.sql import SQLBackend
from .log.timerotating import TimeRotatingLog


log = logging.getLogger(__name__)


class Worker(object):

    def __init__(self, log, backend):
        self.log = log
        self.backend = backend

    def run(self, **kwargs):
        log.info('Worker started processing.')
        for vals in self.log.process(**kwargs):
            log.info('Handling record %r', vals)
            record = Record.from_list(vals)
            self.backend.handle(record)
        log.info('Worker finished processing.')


def main():
    p = argparse.ArgumentParser(description='Run a Manhattan worker with a '
                                'TimeRotatingLog and SQL backend.')
    p.add_argument('-v', '--verbose', dest='loglevel', action='store_const',
                   const=logging.DEBUG, default=logging.WARN,
                   help='Print detailed output')
    p.add_argument('-p', '--path', dest='log_path', type=str,
                   help='Log path')
    p.add_argument('-u', '--url', dest='url', type=str,
                   help='Backend URL')
    p.add_argument('--stay-alive', dest='stay_alive', action='store_true',
                   help='Stay alive and continue processing')

    args = p.parse_args()

    logging.basicConfig(level=args.loglevel)

    backend = SQLBackend(sqlalchemy_url=args.url)
    log = TimeRotatingLog(args.log_path)
    worker = Worker(log, backend)
    worker.run(stay_alive=args.stay_alive)
