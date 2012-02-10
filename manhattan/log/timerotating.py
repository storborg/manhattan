import time
import glob
from fcntl import flock, LOCK_EX, LOCK_UN

from .text import TextLog


class TimeRotatingLog(TextLog):
    sleep_delay = 0.5

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

    def live_iter_glob(self):
        """
        Yield an infinite iterator of the available log file names. If there
        are no new files to yield, just re-yields the most recent one.
        """
        last_consumed = None
        while True:
            fnames = glob.glob('%s.[0-9]*' % self.path)
            fnames.sort()
            fresh_files = False
            for fn in fnames:
                if fn > last_consumed:
                    fresh_files = True
                    last_consumed = fn
                    yield fn
            if not fresh_files:
                if fnames:
                    yield fnames[-1]
                elif self.is_alive:
                    time.sleep(self.sleep_delay)
                else:
                    break

    def tail_glob(self):
        """
        Return an iterator over all the matching log files, yielding a line at
        a time. At the end of all available files, poll the last file for new
        lines and look for new files. If a new file is created, abandon the
        previous file and follow that one.
        """
        fnames = self.live_iter_glob()
        this_file = next(fnames)
        f = open(this_file, 'rb')

        while True:
            start = f.tell()
            line = f.readline()
            if not line:
                next_file = next(fnames)
                if next_file != this_file:
                    this_file = next_file
                    f = open(this_file, 'rb')
                elif self.is_alive:
                    time.sleep(self.sleep_delay)
                    f.seek(start)
                else:
                    break
            else:
                yield line

    def process(self, stay_alive=False):
        records_processed = 0

        self.is_alive = stay_alive

        for line in self.tail_glob():
            yield self.parse(line.strip())
            records_processed += 1
