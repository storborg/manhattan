class TextLog(object):

    def format(self, elements):
        return '\t'.join(el.encode('unicode_escape') for el in elements)

    def parse(self, record):
        return [el.decode('unicode_escape') for el in record.split('\t')]
