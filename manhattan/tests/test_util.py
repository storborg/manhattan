import math
from collections import defaultdict
from unittest import TestCase

from manhattan import util


class TestUtil(TestCase):

    def assertRandomish(self, s, bits=4):
        # Calculate entropy.
        counts = defaultdict(int)
        for char in s:
            counts[char] += 1

        n = float(len(s))
        entropy = sum((c / n) * math.log(n / c) / math.log(2)
                      for c in counts.values())
        self.assertLess(bits - entropy, .01)

    def assertRoughly(self, a, b, f=0.5):
        bot = b - (b * f)
        top = b + (b * f)
        self.assertLessEqual(bot, a)
        self.assertLessEqual(a, top)

    def test_nonce(self):
        n1 = util.nonce()
        n2 = util.nonce()
        self.assertNotEqual(n1, n2)

        s = b''.join(util.nonce() for i in range(100))
        self.assertRandomish(s)

    def test_choose_population_bool(self):
        a = 0
        for ii in range(200):
            if util.choose_population(util.nonce()):
                a += 1
        # Make sure it's relatively uniform...
        self.assertRoughly(a, 100)

    def test_chose_population_bad_value(self):
        with self.assertRaises(ValueError):
            util.choose_population(util.nonce(), 123)

    def test_choose_population_zero_mass(self):
        with self.assertRaises(ValueError):
            util.choose_population(util.nonce(), {'foo': 0})

    def test_choose_population_list(self):
        counts = defaultdict(int)
        for ii in range(300):
            choice = util.choose_population(util.nonce(),
                                            ['foo', 'bar', 'baz'])
            counts[choice] += 1
        self.assertRoughly(counts['foo'], 100)
        self.assertRoughly(counts['bar'], 100)
        self.assertRoughly(counts['baz'], 100)

    def test_choose_population_weighted(self):
        counts = defaultdict(int)
        for ii in range(300):
            choice = util.choose_population(util.nonce(), {'foo': 0.1,
                                                           'quux': 0,
                                                           'bar': 0.1,
                                                           'baz': 0.8})
            counts[choice] += 1
        self.assertRoughly(counts['foo'], 30)
        self.assertRoughly(counts['bar'], 30)
        self.assertRoughly(counts['baz'], 240)
        self.assertEqual(counts['quux'], 0)

    def test_decode_http_header_none(self):
        self.assertEqual(util.decode_http_header(None), u'')

    def test_decode_http_header(self):
        self.assertEqual(util.decode_http_header('hello \xf6 \xe1 world'),
                         u'hello \xf6 \xe1 world')


class TestSigner(TestCase):
    def setUp(self):
        self.sample = util.nonce()

    def test_round_trip(self):
        signer = util.Signer('s3krit')
        signed = signer.sign(self.sample)

        b = signer.unsign(signed)
        self.assertEqual(self.sample, b)

    def test_bad_signature(self):
        signer = util.Signer('s3krit')
        signed = signer.sign(self.sample)

        mangled = signed[:-3]
        with self.assertRaises(util.BadSignature) as cm:
            signer.unsign(mangled)

        self.assertIn(mangled, str(cm.exception))

    def test_lowercase(self):
        signer = util.Signer('s3krit')
        signed = signer.sign(self.sample)

        b = signer.unsign(signed.lower())
        self.assertEqual(self.sample, b)

    def test_uppercase(self):
        signer = util.Signer('s3krit')
        signed = signer.sign(self.sample)

        b = signer.unsign(signed.upper())
        self.assertEqual(self.sample, b)

    def test_bad_data(self):
        signer = util.Signer('s3krit')
        signed = signer.sign(self.sample)

        mangled = signed.split('.')[0]
        with self.assertRaises(util.BadData) as cm:
            signer.unsign(mangled)

        self.assertIn('No separator', str(cm.exception))
        self.assertIn(mangled, str(cm.exception))
