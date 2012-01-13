from collections import defaultdict

from .base import Backend


class MemoryBackend(Backend):

    def __init__(self):
        self.requests = defaultdict(list)
        self.nonbot = set()
        self.goals = defaultdict(set)
        self.populations = defaultdict(set)

    def record_page(self, ts, vid, url):
        self.requests[vid].append((int(ts), url))

    def record_pixel(self, ts, vid):
        self.nonbot.add(vid)

    def record_goal(self, ts, vid, name):
        self.goals[name].append(vid)

    def record_split(self, ts, vid, name, selected):
        self.populations[(name, selected)].add(vid)

    def count(self, goal, variant=None, start=None, end=None):
        """
        Return a count of the number of conversions on a given target.

        :param goal:
          Unicode string, filters to sessions which have converted on the given
          goal.

        :param variant:
          Variant object, filters to sessions which belong to a given variant.

        :param start:
          Timestamp, filters to conversions after this time.

        :param end:
          Timestamp, filters to conversions before this time.
        """
        
        raise NotImplementedError

    def get_sessions(self, target=None, variant=None, start=None, end=None):
        """
        Return a list of session ids which satisfy the given conditions.
        """
        raise NotImplementedError
