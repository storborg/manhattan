class TextLog(object):

    def format(self, elements):
        return '\t'.join(el.encode('string_escape') for el in elements)

    def parse(self, record):
        return [el.decode('string_escape') for el in record.split('\t')]
