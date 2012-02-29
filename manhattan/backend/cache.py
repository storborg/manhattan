from collections import OrderedDict


class DeferredLRUCache(object):
    """
    An LRU cache with deferred (on-request) writing. When values put back into
    the cache, they are flagged as dirty, but are not written to the backend
    until ``.flush()`` is called. When the cache is flushed, all dirty values
    will be written back. Note that ``.flush()`` must be called frequently
    enough that the cache does not self-evict dirty values--this will fail.

    This is NOT thread-safe.
    """
    def __init__(self, get_backend, put_backend, max_size=2000):
        """
        Create a new LRU cache.

        :param get_backend:
            Function which fetches value from the persistent backend.
        :type get_backend:
            Callable with the signature ``func(key)``, returning a ``value``.
        :param put_backend:
            Function which flushes values from the cache to the persistent
            backend.
        :type put_backend:
            Callable with the signature ``func(key, value)``. Return value is
            ignored.
        :param max_size:
            Maximum number of entries to allow in the cache.
        :type max_size:
            int
        """
        self.max_size = max_size
        self.get_backend = get_backend
        self.put_backend = put_backend
        self.entries = OrderedDict()
        self.dirty = set()

    def get(self, key):
        """
        Fetch a value from the cache, reading it from the persistent backend if
        necessary.
        """
        try:
            value = self.entries.pop(key)
        except KeyError:
            value = self.get_backend(key)
        self.entries[key] = value
        self.prune()
        return value

    def put(self, key, value):
        """
        Put a value into the cache, flagging it as dirty to be written back to
        the persistent backend on the next ``flush()`` call.
        """
        if key in self.entries:
            self.entries.pop(key)
        self.entries[key] = value
        self.dirty.add(key)
        self.prune()

    def prune(self):
        """
        Prune the cache object back down to the desired size, if it is larger.
        """
        while len(self.entries) > self.max_size:
            key, value = self.entries.popitem(last=False)
            assert key not in self.dirty

    def flush(self):
        """
        Flush dirty cache entries to the persistent backend.
        """
        to_put = {}
        for key in list(self.dirty):
            to_put[key] = self.entries[key]
        self.put_backend(to_put)
        self.dirty = set()
