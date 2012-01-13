"""
Two "tables"

<time bucket, variant_id, impressions>

<time bucket, variant_id, target, conversions>


Some queries we currently handle....

- # of conversions on target A between time X and time Y.
    --> lookup row key for target A, sum range of counter cols from X to Y

- # of sessions that have activity in the last X minutes which have hit target
A but not target B
    --> ????

- # of sessions with variant Z
    --> lookup row key for variant Z, read counter

- # of conversions on target A with variant Z
    --> lookup row key for Z:A in 'count' CF, read counter

- Average of target A value with variant Z
    --> lookup row key for Z:A in 'value' CF, read counter, divide by # of
    conversions

- Recent sessions
    --> lookup 'any' row key, get list of columns (auto-expiring)

- Exact sessions that have hit target A but not target B

Some queries we want to handle...

- # of sessions assigned to variant Z between time X and time Y
- # of conversions on target A with variant Z between time X and time Y

Stuff should be denormalized to handle these exact queries!
"""

class Backend(object):

    def record_pageview(self, request):
        pass

    def record_pixel(self):
        pass

    def record_goal(self, name):
        pass

    def define_intersection(self, a, b, invert_a=False, invert_b=False):
        pass

    def count(self, target, variant=None, start=None, end=None):
        """
        Return a count of the number of conversions on a given target.

        :param target:
          Unicode string, filters to sessions which have converted on the given
          target.

        :param variant:
          Variant object, filters to sessions which belong to a given variant.

        :param start:
          Datetime, filters to conversions after this time.

        :param end:
          Datetime, filters to conversions before this time.
        """
        pass

    def get_sessions(self, target=None, variant=None, start=None, end=None):
        """
        Return a list of session ids which satisfy the given conditions.
        """


class FakeBackend(Backend):
    pass
