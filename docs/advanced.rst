Advanced Usage
==============

Site Specific Analysis
----------------------

Manhattan can be deployed in an app that handles multiple domains. By default,
all data will be aggregated together. If desired, data can be aggregated by
site using a ``host_map`` passed to ``ManhattanMiddleware``. The host map is
simply a dict mapping the host component of the HTTP URL to an integer site_id,
for example::

    host_map = {
        'foo.com': 1,
        'bar.com': 2,
        'baz.example.edu': 3
    }
    app = ManhattanMiddleware(app, log, secret='s3krit', host_map=host_map)


Configurable Rollups
--------------------

Configurable rollups allow the specification of aggregation time periods or
groups. For example, you can track statistics by:

* Time period (including variable-size periods like 'local timezone months')
* Browser type or version
* IP address or group of IP addresses
* Referring site
* ...anything that can be computed from request headers

For more information see ``manhattan.backend.rollups``.

Complex Goals
-------------

Complex goals are goals/visitor states which can be expressed as a combination
of other goal conversions.

For example, a complex goal *abandoned cart* might refer to the set of visitors
which have hit the *added to cart* goal, but not the *began checkout* goal.

Complex goals can be specified on the command line like::

    --complex="abandoned cart|add to cart|began checkout"
    --complex="hello|foo,bar,baz|quux"
    --complex="name|include1,include2|exclude1,exclude2"

Complex goals will be recorded only if all of the *include* goals have been
satisfied, but none of the *exclude* goals have been satisfied.

When rollups are used, complex goal conversions will be recorded in the rollups
that correspond to the first ``.goal()`` call in which all the *include*
constraints were satisfied.
