Quick Start
===========

Installation
------------

Install with pip::

    $ pip install manhattan


Architecture
------------

Manhattan is broken up into several conceptual modules. Those modules pass data
as follows:

.. graphviz::

    digraph architecture {
        rankdir=LR;

        node [shape=box];

        Middleware -> Log -> Server;

        subgraph cluster_backend {
            label = "Backend";
            Server -> "LRU Cache" -> SQL;
            SQL -> "LRU Cache" -> Server;
        }

        Server -> Client;
        Client -> Server;
    }

The **Log** component of a system can be interchanged, and a few different
options are available for different deployment scenarios.

Frontend Setup
~~~~~~~~~~~~~~

You can set up a typical WSGI application to use Manhattan in a few easy steps,
and be on your way to detailed analytics and easy AB testing. For the sake of
explanation we'll use a very basic 'Hello World' WSGI app. To begin, put the
following into a new file, e.g. ``manhattandemo.py``.::

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
for more information see the ``manhattan.log`` module.::

    from manhattan.middleware import ManhattanMiddleware
    from manhattan.log.timerotating import TimeRotatingLog


    log = TimeRotatingLog('/tmp/manhattan.log')
    app = ManhattanMiddleware(app, log, secret='s3krit')

Try opening up http://localhost:8000 in your browser
and visiting a few urls, e.g.  http://localhost:8000/some-path. Then, look
at the generated files, e.g.::

    $ cat /tmp/manhattan.log.*

You should see log entries from the requests that you just generated.

Goal Conversions and Split Tests
--------------------------------

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

For example, to record a goal conversion, we can modify our basic app like so::

    @wsgify
    def app(req):
        visitor = req.environ['manhattan.visitor']
        if req.path_info == '/pie':
            visitor.goal('pie accomplished')
        return Response('Hello')

After making this change, you should be able to visit
http://localhost:8000/pie, and see an event
recorded in the log for the corresponding goal conversion.

Recording a goal is not idempotent: if you call ``visitor.goal()`` twice, two
goal conversions will be recorded for that visitor. Depending on the particular
analysis being performed, this may affect results.

Performing a split test is similar::

    @wsgify
    def app(req):
        visitor = req.environ['manhattan.visitor']
        if visitor.split('superior dessert preference'):
            s = 'cake'
        else:
            s = 'pie'
        return Response(s)

Visiting http://localhost:8000 should show either 'cake' or 'pie', and record
the returned population in the event log.

Recording a split test is idempotent: for the same visitor and the same test,
the same population will always be returned, so you can make as many successive
calls to ``visitor.split()`` as desired without affecting the results of the
split test.

Backend Setup
-------------

As we've seen, all the frontend does is record events to a log. Although having
the log is useful, in order to do something with the data, we'll want to
aggregate it somehow. This is done by the Manhattan backend, using the
``manhattan-server`` executable.

The backend reconciles events from a log and aggregates the data in-memory,
periodically flushing it to SQL in a denormalized format for result viewing. To
launch the server, pass in a SQLAlchemy-friendly database connection URL and
the log path used by the frontend.::

    $ manhattan-server --path=/tmp/manhattan.log --url=sqlite:///test.db -v

The server will spawn two threads. One thread will begin reconciling the
existing log events, and watch for new events to be recorded. The other thread
will answer aggregate queries over a loopback zeromq connection.

To query the server, start::

    $ manhattan-client

This will provide a python shell with a ``client`` object. Try::

    >>> client.count('pie accomplished')

You can also view conversion statistics for split test populations.::

    >>> client.count('pie accomplished',
                     variant=('superior dessert preference', 'True'))

You'll probably want to be able to query analytics results from within another
application. The same ``client`` object is also available inside other python
processes with just::

    from manhattan.client import Client

    client = Client()

Next Steps
----------

For more sophisticated production analytics, check out the Advanced Usage section.
