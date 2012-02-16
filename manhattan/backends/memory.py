from collections import defaultdict


class MemoryBackend(object):

    def __init__(self):
        self._nonbot = set()
        self._visitors = {}
        self._goals = defaultdict(set)
        self._vids_by_variant = defaultdict(set)
        self._variants_by_vid = defaultdict(set)
        self._all = set()
        self._ptr = None

    def handle(self, rec, ptr):
        assert rec.key in ('page', 'pixel', 'goal', 'split')

        if rec.key == 'page':
            self._goals['viewed page'].add(rec.vid)

            self._visitors[rec.vid] = dict(ip=rec.ip,
                                           user_agent=rec.user_agent)
            self._all.add(rec.vid)

        elif rec.key == 'pixel':
            self._nonbot.add(rec.vid)

        elif rec.key == 'goal':
            self._goals[rec.name].add(rec.vid)

        else:  # split
            variant = rec.test_name, rec.selected
            self._vids_by_variant[variant].add(rec.vid)
            self._variants_by_vid[rec.vid].add(variant)

        self._ptr = ptr

    def get_pointer(self):
        return self._ptr

    def count(self, goal, variant=None):
        """
        Return a count of the number of conversions on a given target.

        :param goal:
          Unicode string, filters to sessions which have converted on the given
          goal.

        :param variant:
          Variant object, filters to sessions which belong to a given variant.
        """
        sessions = self._goals[goal]

        if variant:
            sessions &= self._vids_by_variant[variant]

        sessions &= self._nonbot

        return len(sessions)

    def get_sessions(self, goal=None, variant=None):
        """
        Return a list of session ids which satisfy the given conditions.
        """
        if goal and variant:
            sessions = self._goals[goal] & self._vids_by_variant[variant]
        elif goal:
            sessions = self._goals[goal]
        elif variant:
            sessions = self._vids_by_variant[variant]
        else:
            sessions = self._all

        sessions &= self._nonbot

        return sessions
