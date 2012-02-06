import time
import glob
from fcntl import flock, LOCK_EX, LOCK_UN

from .text import TextLog


class TimeRotatingLog(TextLog):

    def __init__(self, path):
        self.path = path
        self.current_log_name = None

    def log_name_for(self, ts):
        ts = int(ts)
        start = ts - (ts % 3600)
        return '%s.%s' % (self.path, start)

    def write(self, elements):
        check_log_name = self.log_name_for(time.time())
        if check_log_name != self.current_log_name:
            self.current_log_name = check_log_name
            self.f = open(self.current_log_name, 'ab')

        record = self.format(elements)
        assert '\n' not in record

        flock(self.f, LOCK_EX)
        self.f.write(record)
        self.f.write('\n')
        flock(self.f, LOCK_UN)

    def process(self):
        """
        For now this just supports a very simple 'process all the written logs'
        kind of behavior. Ultimately it should tail -f the matching glob and
        stay alive, as well as support processing from a certain timestamp or
        log file.
        """
        fnames = glob.glob('%s.[0-9]*' % self.path)
        records_processed = 0

        for fname in fnames:
            f = open(fname, 'rb')

            for line in f.readlines():
                yield self.parse(line.strip())
                records_processed += 1
