from collections import defaultdict

from .base import Backend


class MemoryBackend(Backend):

    def __init__(self):
        self.requests = defaultdict(list)
        self.nonbot = set()
        self.goals = defaultdict(set)
        self.populations = defaultdict(set)
        self.all = set()

    def record_page(self, ts, vid, url):
        self.goals['viewed page'].add(vid)
        self.requests[vid].append((int(ts), url))
        self.all.add(vid)

    def record_pixel(self, ts, vid):
        self.nonbot.add(vid)

    def record_goal(self, ts, vid, name, value):
        self.goals[name].add(vid)

    def record_split(self, ts, vid, name, selected):
        self.populations[(name, selected)].add(vid)

    def count(self, goal, variant=None):
        """
        Return a count of the number of conversions on a given target.

        :param goal:
          Unicode string, filters to sessions which have converted on the given
          goal.

        :param variant:
          Variant object, filters to sessions which belong to a given variant.
        """
        sessions = self.goals[goal]

        if variant:
            sessions &= self.populations[variant]

        return len(sessions)

    def get_sessions(self, goal=None, variant=None):
        """
        Return a list of session ids which satisfy the given conditions.
        """
        if goal and variant:
            sessions = self.goals[goal] & self.populations[variant]
        elif goal:
            sessions = self.goals[goal]
        elif variant:
            sessions = self.populations[variant]
        else:
            sessions = self.all
        return sessions
