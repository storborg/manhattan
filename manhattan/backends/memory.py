from collections import defaultdict

from .base import Backend


class MemoryBackend(Backend):

    def __init__(self):
        self.requests = defaultdict(list)
        self.nonbot = set()
        self.goals = defaultdict(set)
        self.populations = defaultdict(set)

    def record_page(self, ts, id, url):
        self.requests[id].append((ts, url))

    def record_pixel(self, ts, id):
        self.nonbot.add(id)

    def record_goal(self, ts, id, name):
        self.goals[name].add(id)

    def record_split(self, ts, id, name, selected):
        self.populations[(name, selected)].add(id)
