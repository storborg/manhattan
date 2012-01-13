from manhattan.backends.base import Backend


class SQLBackend(Backend):

    def record_pageview(self, request):
        raise NotImplementedError

    def record_pixel(self):
        raise NotImplementedError

    def record_goal(self, name):
        raise NotImplementedError
