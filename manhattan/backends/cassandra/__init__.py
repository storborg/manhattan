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
from manhattan.backends.base import Backend


class CassandraBackend(Backend):
    pass
