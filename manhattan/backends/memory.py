from collections import defaultdict


class MemoryBackend(object):

    def __init__(self):
        self._nonbot = set()
        self._nonbot_queue = defaultdict(list)

        self._visitors = {}
        self._goals = defaultdict(set)

        self._variants_by_test = defaultdict(set)
        self._test_starts = {}
        self._test_lasts = {}

        self._vids_by_variant = defaultdict(set)
        self._variants_by_vid = defaultdict(set)
        self._all = set()

        self._ptr = None

    def handle(self, rec, ptr):
        if rec.key == 'pixel':
            self._nonbot.add(rec.vid)
            for rec in self._nonbot_queue.pop(rec.vid, ()):
                self.handle_nonbot(rec)

        elif rec.vid in self._nonbot:
            self.handle_nonbot(rec)

        else:
            self._nonbot_queue[rec.vid].append(rec)

        self._ptr = ptr

    def handle_nonbot(self, rec):
        assert rec.key in ('page', 'goal', 'split')

        ts = int(float(rec.timestamp))

        if rec.key == 'page':
            self._goals[u'viewed page'].add(rec.vid)

            self._visitors[rec.vid] = dict(ip=rec.ip,
                                           user_agent=rec.user_agent)
            self._all.add(rec.vid)

        elif rec.key == 'goal':
            self._goals[rec.name].add(rec.vid)

        else:  # split
            self._variants_by_test[rec.test_name].add(rec.selected)
            variant = rec.test_name, rec.selected
            self._vids_by_variant[variant].add(rec.vid)
            self._variants_by_vid[rec.vid].add(variant)

            self._test_starts.setdefault(rec.test_name, ts)
            self._test_lasts[rec.test_name] = ts

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
            sessions = sessions & self._vids_by_variant[tuple(variant)]

        sessions = sessions & self._nonbot

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

        sessions = sessions & self._nonbot

        return sessions

    def tests(self):
        """
        Return a list of (test name, first_timestamp, last_timestamp) tuples.
        """
        ret = []
        for test_name, start_ts in self._test_starts.iteritems():
            ret.append((test_name, start_ts, self._test_lasts[test_name]))
        return ret

    def test_results(self, test_name):
        """
        Return a list of (variant name, conversions by goal dict)
        """
        ret = []
        for selected in self._variants_by_test[test_name]:
            conversions = {}
            for goal in self._goals.keys():
                conversions[goal] = \
                        self.count(goal, variant=(test_name, selected))
            ret.append((selected, conversions))
        return ret
