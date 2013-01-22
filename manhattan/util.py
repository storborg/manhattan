"""
Various utility functions used by Manhattan. It is not expected that these will
be used externally.
"""

import os
import random
import bisect
import hmac
import hashlib
import binascii


"""
A 1 pixel transparent GIF as a bytestring. For use as a tracking "beacon" in an
HTML document.
"""
transparent_pixel = (
    'GIF89a'
    '\x01\x00\x01\x00\x80\x00\x00\xff\xff\xff\x00\x00\x00!\xf9\x04'
    '\x00\x00\x00\x00\x00,\x00\x00\x00\x00\x01\x00\x01\x00\x00\x02'
    '\x02D\x01\x00;')


def pixel_tag(path):
    """
    Build the HTML surrounding a given image path that will be injected as a
    tracking pixel.

    :param path:
        URL path to the tracking pixel.
    :type request:
        string
    :returns:
        HTML markup for image tag.
    :rtype:
        string
    """
    return ('<img style="height:0;width:0;position:absolute;" '
            'src="%s" alt="" />' % path)


def nonrandom_choice(seed, seq):
    """
    Pick an element from the specified sequence ``seq`` based on the value of
    the specified string ``seed``. Guaranteed to be deterministic, tries to be
    uniform.

    :param seed:
        Any string specifier to control the element.
    :type seed:
        string
    :param seq:
        Python sequence to pick from.
    :type seq:
        sequence object
    :returns:
        An element from ``seq``.
    """
    return random.Random(seed).choice(seq)


def nonrandom(seed, n):
    """
    Return a deterministic but pseudo-random number between 0 and ``n``, based
    on the value of ``seed``. Guaranteed to be deterministic, tries to be
    uniform.

    :param seed:
        Any string specifier to control the output.
    :type seed:
        string
    :param n:
        Range of output value.
    :type n:
        float
    :returns:
        Float between 0 and ``n``.
    :rtype:
        float
    """
    return random.Random(seed).random() * n


def choose_population(seed, populations=None):
    """
    Randomly pick an element from populations according to type.

    :param seed:
        Any string specifier to control the output.
    :type seed:
        string
    :param populations:
        If not specified, perform a straight AB test between True and False. If
        specified as a sequence, pick uniformly from the sequence. If specified
        as a dict, use the keys as population names and the values as the
        weight that each population should receive, and distribute between them
        according to weight.
    :type populations:
        None, sequence, or dict
    :returns:
        Selected population
    """
    # Uniform distribution between True, False
    if populations is None:
        return nonrandom_choice(seed, (True, False))

    # Uniform distribution over populations
    if isinstance(populations, list):
        return nonrandom_choice(seed, populations)

    # Weighted distribution over populations
    if isinstance(populations, dict):
        pop_name = []
        pop_mass = []
        running_mass = 0
        for name, mass in populations.iteritems():
            if mass > 0:
                pop_name.append(name)
                pop_mass.append(running_mass)
                running_mass += mass

        if running_mass == 0:
            raise ValueError("Need at least one option with probability > 0")

        r = nonrandom(seed, running_mass)
        i = bisect.bisect(pop_mass, r) - 1
        return pop_name[i]

    raise ValueError("Invalid population description")


def decode_http_header(raw):
    """
    Decode a raw HTTP header into a unicode string. RFC 2616 specifies that
    they should be latin1-encoded (a.k.a. iso-8859-1). If the passed-in value
    is None, return an empty unicode string.

    :param raw:
        Raw HTTP header string.
    :type raw:
        string (non-unicode)
    :returns:
        Decoded HTTP header.
    :rtype:
        unicode string
    """
    if raw:
        return raw.decode('iso-8859-1', 'replace')
    else:
        return u''


def decode_url(raw):
    """
    Decode a URL into a unicode string. Expected to be UTF-8.

    :param raw:
        Raw URL string.
    :type raw:
        string (non-unicode)
    :returns:
        Decode URL.
    :rtype:
        unicode string
    """
    return raw.decode('utf-8')


def constant_time_compare(a, b):
    "Compare two strings with constant time. Used to prevent timing attacks."
    if len(a) != len(b):
        return False
    result = 0
    for x, y in zip(a, b):
        result |= ord(x) ^ ord(y)
    return result == 0


def nonce():
    """
    Build a cryptographically random nonce.

    :returns:
        Hex string, with 20 bytes (40 hex chars).
    :rtype:
        string
    """
    return binascii.hexlify(os.urandom(20))


class SignerError(Exception):
    pass


class BadData(SignerError):
    pass


class BadSignature(SignerError):
    pass


class Signer(object):
    def __init__(self, secret):
        self.key = hashlib.sha1('manhattan.signer.' + secret).digest()
        self.sep = '.'

    def get_signature(self, value):
        "Compute the signature for the given value."
        mac = hmac.new(self.key, msg=value, digestmod=hashlib.sha1)
        return binascii.hexlify(mac.digest())

    def sign(self, value):
        return "%s%s%s" % (value, self.sep, self.get_signature(value))

    def unsign(self, signed_value):
        if self.sep not in signed_value:
            raise BadData('No separator %r found in cookie: %s' %
                          (self.sep, signed_value))

        signed_value = signed_value.lower()
        value, sig = signed_value.rsplit(self.sep, 1)
        expected = self.get_signature(value)

        if constant_time_compare(sig, expected):
            return value

        s = ('Signature for cookie %r does not match: expected %r, '
             'got %s' % (signed_value, expected, sig))
        raise BadSignature(s)
