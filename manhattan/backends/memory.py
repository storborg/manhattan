from collections import defaultdict

from .base import Backend


class MemoryBackend(Backend):

    def __init__(self):
        self.requests = defaultdict(list)
        self.nonbot = set()
        self.visitors = {}
        self.goals = defaultdict(set)
        self.populations = defaultdict(set)
        self.all = set()

    def handle(self, rec):
        assert rec.key in ('page', 'pixel', 'goal', 'split')

        if rec.key == 'page':
            ts = int(float(rec.timestamp))
            self.goals['viewed page'].add(rec.vid)

            self.visitors[rec.vid] = dict(ip=rec.ip, user_agent=rec.user_agent)
            self.requests[rec.vid].append((ts, rec.url, rec.ip, rec.method))
            self.all.add(rec.vid)

        elif rec.key == 'pixel':
            self.nonbot.add(rec.vid)

        elif rec.key == 'goal':
            self.goals[rec.name].add(rec.vid)

        else:  # split
            self.populations[(rec.test_name, rec.selected)].add(rec.vid)

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
