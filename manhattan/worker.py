import time
import logging
import logging.config
import code
import argparse

from .record import Record
from .backends.sql import SQLBackend
from .backends.memory import MemoryBackend
from .log.timerotating import TimeRotatingLog


log = logging.getLogger(__name__)


class Worker(object):

    def __init__(self, log, backend, stats_every=50):
        self.log = log
        self.backend = backend
        self.stats_every = stats_every

        self.last_live_ts = None
        self.last_record_ts = None
        self.last_num_records = None

    def dump_stats(self, num_records, record_ts):
        live_ts = time.time()
        if self.last_live_ts:
            live_elapsed = live_ts - self.last_live_ts
            record_elapsed = record_ts - self.last_record_ts

            record_rate = ((num_records - self.last_num_records) /
                           float(live_elapsed))

            clock_rate = float(record_elapsed) / float(live_elapsed)
            secs_behind = live_ts - record_ts
            clock_eta = secs_behind / clock_rate

            log.info('Processed %d records, %0.1f /sec, %0.1fx realtime, '
                     '%d secs behind, ETA: %0.1f seconds',
                     num_records, record_rate, clock_rate, secs_behind,
                     clock_eta)

        self.last_live_ts = live_ts
        self.last_record_ts = record_ts
        self.last_num_records = num_records

    def run(self, resume=True, **kwargs):
        log.info('Worker started processing.')

        if resume:
            kwargs['process_from'] = self.backend.get_pointer()
            log.info('Resuming from %s', kwargs['process_from'])

        for ii, (vals, pointer) in enumerate(self.log.process(**kwargs)):
            record = Record.from_list(vals)
            self.backend.handle(record, pointer)
            if (ii % self.stats_every) == 0:
                self.dump_stats(ii, int(float(record.timestamp)))

        log.info('Worker finished processing.')


def main():
    p = argparse.ArgumentParser(description='Run a Manhattan worker with a '
                                'TimeRotatingLog and SQL backend.')
    p.add_argument('-v', '--verbose', dest='loglevel', action='store_const',
                   const=logging.DEBUG, default=logging.WARN,
                   help='Print detailed output')
    p.add_argument('-p', '--path', dest='log_path', type=str,
                   help='Log path')
    p.add_argument('-b', '--backend', dest='backend',
                   type=str, default='sql', choices=('memory', 'sql'),
                   help='Backend type')
    p.add_argument('-u', '--url', dest='url', type=str,
                   help='SQL Backend URL')
    p.add_argument('--stay-alive', dest='stay_alive', action='store_true',
                   help='Stay alive and continue processing')

    args = p.parse_args()

    logging.basicConfig(level=args.loglevel)

    if args.backend == 'memory':
        backend = MemoryBackend()
        stats_every = 5000
    else:
        backend = SQLBackend(sqlalchemy_url=args.url)
        stats_every = 50

    log = TimeRotatingLog(args.log_path)
    worker = Worker(log, backend, stats_every=stats_every)
    worker.run(stay_alive=args.stay_alive)

    if args.backend == 'memory':
        code.interact(banner="""Manhattan backend shell

          The MemoryBackend instance is available as 'backend'
        """, local=dict(backend=backend))
