from collections import OrderedDict


class DeferredLRUCache(object):
    """
    An LRU cache with deferred (on-request) writing.

    NOT thread-safe.
    """
    def __init__(self, get_backend, put_backend, max_size=2000):
        self.max_size = max_size
        self.get_backend = get_backend
        self.put_backend = put_backend
        self.entries = OrderedDict()
        self.dirty = set()

    def get(self, key):
        try:
            value = self.entries.pop(key)
        except KeyError:
            value = self.get_backend(key)
        self.entries[key] = value
        self.prune()
        return value

    def put(self, key, value):
        if key in self.entries:
            self.entries.pop(key)
        self.entries[key] = value
        self.dirty.add(key)
        self.prune()

    def prune(self):
        while len(self.entries) > self.max_size:
            key, value = self.entries.popitem(last=False)
            if key in self.dirty:
                import pdb
                pdb.set_trace()
            assert key not in self.dirty

    def flush(self):
        to_put = {}
        for key in list(self.dirty):
            to_put[key] = self.entries[key]
        self.put_backend(to_put)
        self.dirty = set()
