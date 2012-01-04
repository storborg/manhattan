from unittest import TestCase

from manhattan import util


class TestUtil(TestCase):

    def test_nonce(self):
        calls = [util.nonce() for x in xrange(10)]
        self.assertEqual(len(set(calls)), len(calls))
        self.assertGreater(len(calls[0]), 8)

    def test_nonrandom_choice(self):
        pass

    def test_nonrandom(self):
        pass

    def test_choose_population(self):
        pass
