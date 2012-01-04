"""
Two "tables":

    <time bucket, variant_id, impressions>

    <time bucket, variant_id, target, conversions>
"""


class Backend(object):

    def pageview(self, request):
        pass

    def pixel(self):
        pass

    def goal(self, name):
        pass


class FakeBackend(Backend):
    pass
