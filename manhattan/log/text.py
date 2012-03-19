class TextLog(object):
    """
    A log superclass which provides string handling methods, intended for logs
    which will use line-based text. This class does not implement a usable log
    by itself, and is intended to be subclassed.
    """
    def format(self, elements):
        return b'\t'.join(el.encode('unicode_escape') for el in elements)

    def parse(self, record):
        return [el.decode('unicode_escape') for el in record.split(b'\t')]
