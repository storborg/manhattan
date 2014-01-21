Manhattan - Easy, High Performance Analytics
============================================

Scott Torborg - `Cart Logic <http://www.cartlogic.com>`_

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
* 1/2 oz sweet vermouth
* 2 dashes bitters
* 1 cherry

Example Usage
-------------

.. code-block:: python

    from manhattan.middleware import ManhattanMiddleware
    from manhattan.log.timerotating import TimeRotatingLog

    log = TimeRotatingLog('/tmp/manhattan.log')
    app = ManhattanMiddleware(app, log, secret='s3krit')

.. code-block:: python

    def checkout_action(request):
        request.visitor.goal('Began Checkout')
        if request.visitor.split('Blue Button'):
            ...


Contents
--------

.. toctree::
    :maxdepth: 2

    api
    quickstart
    advanced
    contributing
