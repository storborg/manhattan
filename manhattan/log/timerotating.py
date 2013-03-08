import os
import os.path
import time
import glob
from fcntl import flock, LOCK_EX, LOCK_UN
from threading import Event

from .text import TextLog


class TimeRotatingLog(TextLog):
    """
    A type of log which writes records as individual lines to a series of
    files, with one file per hour of time in which events occur. Files are
    written atomically (using ``fcntl.flock``) and only appended to.
    """
    sleep_delay = 0.5

    def __init__(self, path):
        self.path = path
        self.current_log_name = None
        self.killed = Event()
        self.f = None

    def create_dirs(self):
        dirpath = os.path.dirname(self.path)
        if dirpath and not os.path.exists(dirpath):
            os.makedirs(dirpath)

    def log_name_for(self, ts):
        ts = int(ts)
        start = ts - (ts % 3600)
        return '%s.%s' % (self.path, start)

    def write(self, *records):
        check_log_name = self.log_name_for(time.time())
        if check_log_name != self.current_log_name:
            self.current_log_name = check_log_name
            self.create_dirs()
            if self.f:
                self.f.close()
            self.f = open(self.current_log_name, 'ab')

        data = [self.format(r) for r in records]
        for r in data:
            assert b'\n' not in r, '\\n found in %r' % r
        data.append('')  # to get the final \n
        data = b'\n'.join(data)

        flock(self.f, LOCK_EX)
        self.f.write(data)
        self.f.flush()
        flock(self.f, LOCK_UN)

    def live_iter_glob(self, start_file):
        """
        Yield an infinite iterator of the available log file names. If there
        are no new files to yield, just re-yields the most recent one.
        """
        last_consumed = None
        while True:
            fnames = glob.glob('%s.[0-9]*' % self.path)
            fnames.sort()

            # Crop fnames to start at ``start_file`` if it is supplied.
            if start_file and (start_file in fnames):
                fnames = fnames[fnames.index(start_file):]

            fresh_files = False
            for fn in fnames:
                if (not last_consumed) or (fn > last_consumed):
                    fresh_files = True
                    last_consumed = fn
                    yield fn
            if not fresh_files:
                if fnames:
                    yield fnames[-1]
                elif not self.killed.is_set():
                    time.sleep(self.sleep_delay)
                else:
                    break

    def tail_glob(self, start_file, start_offset):
        """
        Return an iterator over all the matching log files, yielding a line at
        a time. At the end of all available files, poll the last file for new
        lines and look for new files. If a new file is created, abandon the
        previous file and follow that one.
        """
        fnames = self.live_iter_glob(start_file=start_file)
        this_file = next(fnames)
        f = open(this_file, 'rb')

        if start_offset:
            f.seek(start_offset)

        while True:
            start = f.tell()
            line = f.readline()
            if not line:
                next_file = next(fnames)
                if next_file != this_file:
                    this_file = next_file
                    f = open(this_file, 'rb')
                elif not self.killed.is_set():
                    time.sleep(self.sleep_delay)
                    f.seek(start)
                else:
                    break
            else:
                pointer = '%s:%d' % (this_file, f.tell())
                yield line, pointer

    def process(self, process_from=None, stay_alive=False, killed_event=None):
        if not stay_alive:
            self.killed.set()
        if killed_event:
            self.killed = killed_event

        if process_from:
            start_file, start_offset = process_from.rsplit(':', 1)
            start_offset = int(start_offset)
        else:
            start_file = start_offset = None

        for line, pointer in self.tail_glob(start_file=start_file,
                                            start_offset=start_offset):
            yield self.parse(line.strip()), pointer
