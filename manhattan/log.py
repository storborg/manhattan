from gzlog import GZLog


class EventLog(object):

    def __init__(self, path):
        self.writer = GZLog(path)

    def format(self, elements):
        return '\t'.join(el.replace('\t', ' ') for el in elements)

    def parse(self, record):
        return record.split('\t')

    def write(self, elements):
        self.writer.write(self.format(elements))

    def process(self):
        self.writer.rotate()
        for record in self.writer.read():
            yield self.parse(record)
