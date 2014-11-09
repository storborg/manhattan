from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import time

from manhattan.client import Client


client = Client()


def test(f, trials=500):
    start = time.time()
    for ii in range(trials):
        f()
    end = time.time()
    elapsed = end - start
    print("Ran %d trials, %0.2f ms each" %
          (trials, ((1000. * elapsed) / trials)))


def get_results():
    client.test_results('Discount Rate for Datrek')


def get_tests():
    client.tests()


if __name__ == '__main__':
    print("Testing tests list.")
    test(get_tests)
    print("Testing resuls page.")
    test(get_results)
