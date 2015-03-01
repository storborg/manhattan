Manhattan - Robust Server-Side Analytics
========================================

.. image:: https://secure.travis-ci.org/storborg/manhattan.png
    :target: http://travis-ci.org/storborg/manhattan
.. image:: https://coveralls.io/repos/storborg/manhattan/badge.png?branch=master
    :target: https://coveralls.io/r/storborg/manhattan
.. image:: https://pypip.in/v/manhattan/badge.png
    :target: https://crate.io/packages/manhattan
.. image:: https://pypip.in/d/manhattan/badge.png
    :target: https://crate.io/packages/manhattan

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
* &frac12; oz sweet vermouth
* 2 dashes bitters
* 1 cherry


Installation
============

Install with pip::

    $ pip install manhattan


Documentation
=============

Manhattan has `extensive documentation here <http://www.cartlogic.com/manhattan>`_.


License
=======

Manhattan is licensed under an MIT license. Please see the LICENSE file for
more information.


Code Standards
==============

Manhattan has a comprehensive test suite with 100% line and branch coverage, as
reported by the excellent ``coverage`` module. To run the tests, simply run in
the top level of the repo::

    $ nosetests

There are no `PEP8 <http://www.python.org/dev/peps/pep-0008/>`_ or
`Pyflakes <http://pypi.python.org/pypi/pyflakes>`_ warnings in the codebase. To
verify that::

    $ pip install pep8 pyflakes
    $ pep8 .
    $ pyflakes .

Any pull requests must maintain the sanctity of these three pillars.
