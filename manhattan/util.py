import os
import random
import hashlib
import bisect


transparent_pixel = (
    'GIF89a'
    '\x01\x00\x01\x00\x80\x00\x00\xff\xff\xff\x00\x00\x00!\xf9\x04'
    '\x00\x00\x00\x00\x00,\x00\x00\x00\x00\x01\x00\x01\x00\x00\x02'
    '\x02D\x01\x00;')


def pixel_tag(path):
    return '<img src="%s" alt="" />' % path


def nonce():
    """
    Return a random nonce, 160 bits long as a hex string.
    """
    return os.urandom(20).encode('hex')


def nonrandom_choice(s, seq):
    """
    Pick an element from ``seq`` according to the value of string ``s``.
    Guaranteed to be deterministic, tries to be uniform.
    """
    return random.Random(s).choice(seq)


def nonrandom(s, n):
    """
    Return a deterministic but pseudo-random number between 0 and ``n``.
    """
    return random.Random(s).random() * n


def choose_population(s, populations=None):
    """
    Randomly pick an element from populations according to type.
    """
    # Uniform distribution between True, False
    if populations is None:
        return nonrandom_choice(s, (True, False))

    # Uniform distribution over populations
    if isinstance(populations, list):
        return nonrandom_choice(s, populations)

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

        r = nonrandom(s, running_mass)
        i = bisect.bisect(pop_mass, r) - 1
        return pop_name[i]

    raise ValueError("Invalid population description")
