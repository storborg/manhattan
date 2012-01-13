from collections import deque


class MemoryLog(object):
    def __init__(self):
        self.q = deque()

    def write(self, elements):
        self.q.append(elements)

    def process(self):
        to_process = self.q
        self.q = deque()
        for record in to_process:
            yield record
