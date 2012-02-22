import os
import random
import bisect


"""
A 1 pixel transparent GIF as a bytestring.
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
    return '<img src="%s" alt="" />' % path


def nonce():
    """
    Build a cryptographically random nonce.

    :returns:
        Hex string, with 20 bytes (40 hex chars).
    :rtype:
        string
    """
    return os.urandom(20).encode('hex')


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
