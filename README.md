&#127864; Manhattan - Robust Server-Side Analytics
==================================================

Scott Torborg - [Cart Logic](http://www.cartlogic.com)

Manhattan is a Python infrastructure block to provide basic server-side
analytics and multivariate testing. It is:

* **Easy** to deploy and develop on
* **Scalable** 
* **Not slow**, in-band request latency < 1ms and can process > 2k events/sec
* **Customizable** and flexible to varying rollup needs
* **Robust** to server failures, migrating between cluster topologies, and
  backend reconfiguration

It is also:

* 2 oz rye whiskey
* &frac12; oz sweet vermouth
* 2 dashes bitters
* 1 cherry


Installation
============

    $ pip install manhattan


Quick Start
===========

### Frontend Setup ###

You can set up a typical WSGI application to use Manhattan in a few easy steps,
and be on your way to detailed analytics and easy AB testing. For the sake of
explanation we'll use a very basic 'Hello World' WSGI app. To begin, put the
following into a new file, e.g. ``manhattandemo.py``.

    from webob import Response
    from webob.dec import wsgify


    @wsgify
    def app(req):
        return Response('Hello')


    if __name__ == '__main__':
        from wsgiref.simple_server import make_server
        httpd = make_server('', 8000, app)
        print "Serving on 0.0.0.0:8000..."
        httpd.serve_forever()

Wrap the WSGI application in the ``ManhattanMiddleware``, setting up a
``TimeRotatingLog`` instance for the application to log events to.

The Manhattan frontend (middleware) is completely stateless, and simply logs
events to a log instance for later reconciliation. This ensures that the
additional in-band request latency incurred by adding Manhattan to an
application is very small: typically less than 1ms.

The recommended log type for basic deployments is the ``TimeRotatingLog``,
which writes events as plaintext lines in a series of append-only files, with
one file per hour. There are other choices of logs for advanced deployments,
for more information see the ``manhattan.log`` module.

    from manhattan.middleware import ManhattanMiddleware
    from manhattan.log.timerotating import TimeRotatingLog


    log = TimeRotatingLog('/tmp/manhattan.log')
    app = ManhattanMiddleware(app, log)

Try opening up [http://localhost:8000](http://localhost:8000) in your browser
and visiting a few urls, e.g.
[http://localhost:8000/some-path](http://localhost:8000/some-path). Then, look
at the generated files, e.g.:

    $ cat /tmp/manhattan.log.*

You should see log entries from the requests that you just generated.

### Goal Conversions and Split Tests ###

The ``ManhattanMiddleware`` places a key in the WSGI environ which acts as a
handle to perform testing operations. This handle is called the ``visitor`` and
is an instance of ``manhattan.visitor.Visitor``. There are three types of
operations you can perform on this handle.

* ``visitor.page(req)`` - Record a page view, passing in a ``req`` object that
  is an instance of ``webob.Request``. This event is recorded implicitly on
  every web request which uses the middleware, and does not need to be done by
  the wrapped application unless additional page view records are desired.
* ``visitor.pixel()`` - Record that this visitor has requested a tracking
  pixel.  This is used to exclude events from visitors which either don't
  request images or don't support cookies (both likely symptoms of a bot). This
  event is record implicitly by the middleware, and does not need to be done by
  the wrapped application.
* ``visitor.goal(name, value=None, value_type=None, value_format=None)`` -
  Record a goal conversion, where ``name`` is a string describing the goal.
  ``value`` and associated parameters are optional.
* ``visitor.split(test_name, populations=None)`` - Perform a split test, record
  the population assigned to this visitor, and return it. In the most basic
  form, with no ``populations`` argument specified, this just does a 50/50 AB
  test and returns True or False to indicate the assigned population.

For example, to record a goal conversion, we can modify our basic app like so:

    @wsgify
    def app(req):
        visitor = req.environ['manhattan.visitor']
        if req.path_info == '/pie':
            visitor.goal('pie accomplished')
        return Response('Hello')

After making this change, you should be able to visit
[http://localhost:8000/pie](http://localhost:8000/pie), and see an event
recorded in the log for the corresponding goal conversion.

Recording a goal is not idempotent: if you call ``visitor.goal()`` twice, two
goal conversions will be recorded for that visitor. Depending on the particular
analysis being performed, this may affect results.

Performing a split test is similar:

    @wsgify
    def app(req):
        visitor = req.environ['manhattan.visitor']
        if visitor.split('superior dessert preference'):
            s = 'cake'
        else:
            s = 'pie'
        return Response(s)

Visiting [http://localhost:8000](http://localhost:8000) should show either
'cake' or 'pie', and record the returned population in the event log.

Recording a split test is idempotent: for the same visitor and the same test,
the same population will always be returned, so you can make as many successive
calls to ``visitor.split()`` as desired without affecting the results of the
split test.

### Backend Setup ###

As we've seen, all the frontend does is record events to a log. Although having
the log is useful, in order to do something with the data, we'll want to
aggregate it somehow. This is done by the Manhattan backend, using the
``manhattan-server`` executable.

The backend reconciles events from a log and aggregates the data in-memory,
periodically flushing it to SQL in a denormalized format for result viewing. To
launch the server, pass in a SQLAlchemy-friendly database connection URL and
the log path used by the frontend.

    $ manhattan-server --path=/tmp/manhattan.log --url=sqlite:///test.db -v

The server will spawn two threads. One thread will begin reconciling the
existing log events, and watch for new events to be recorded. The other thread
will answer aggregate queries over a loopback zeromq connection.

To query the server, start:

    $ manhattan-client

This will provide a python shell with a ``client`` object. Try:

    >>> client.count('pie accomplished')

You can also view conversion statistics for split test populations.

    >>> client.count('pie accomplished',
                     variant=('superior dessert preference', 'True'))

You'll probably want to be able to query analytics results from within another
application. The same ``client`` object is also available inside other python
processes with just:

    from manhattan.client import Client

    client = Client()

### Next Steps ###

For more sophisticated production analytics there are two important features:

#### Site Specific Analysis ####

Manhattan can be deployed in an app that handles multiple domains. By default, all data will be aggregated together. If desired, data can be aggregated by site using a ``host_map`` passed to ``ManhattanMiddleware``. The host map is simply a dict mapping the host component of the HTTP URL to an integer site_id, for example:

    host_map = {
        'foo.com': 1,
        'bar.com': 2,
        'baz.example.edu': 3
    }
    app = ManhattanMiddleware(app, log, host_map=host_map)


#### Configurable Rollups ####

Configurable rollups allow the specification of aggregation time periods or
groups. For example, you can track statistics by:

* Time period (including variable-size periods like 'local timezone months')
* Browser type or version
* IP address or group of IP addresses
* Referring site
* ...anything that can be computed from request headers

For more information see ``manhattan.backend.rollups``.

#### Complex Goals ####

Complex goals are goals/visitor states which can be expressed as a combination
of other goal conversions.

For example, a complex goal *abandoned cart* might refer to the set of visitors
which have hit the *added to cart* goal, but not the *began checkout* goal.

Complex goals can be specified on the command line like

    --complex="abandoned cart|add to cart|began checkout"
    --complex="hello|foo,bar,baz|quux"
    --complex="name|include1,include2|exclude1,exclude2"

Complex goals will be recorded only if all of the *include* goals have been
satisfied, but none of the *exclude* goals have been satisfied.

When rollups are used, complex goal conversions will be recorded in the rollups
that correspond to the first ``.goal()`` call in which all the *include*
constraints were satisfied.


Code Standards
==============

Manhattan has a comprehensive test suite with 100% line and branch coverage, as
reported by the excellent ``coverage`` module. To run the tests, simply run in
the top level of the repo:

    $ nosetests

There are no [PEP8](http://www.python.org/dev/peps/pep-0008/) or
[Pyflakes](http://pypi.python.org/pypi/pyflakes) warnings in the codebase. To
verify that:

    $ pip install pep8 pyflakes
    $ pep8 -r .
    $ pyflakes .

Any pull requests must maintain the sanctity of these three pillars.
