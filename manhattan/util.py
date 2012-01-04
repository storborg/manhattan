import random
import hashlib
import bisect


def nonce():
    """
    Return a random nonce.
    """
    r = random.random()
    return hashlib.sha1('%.66f' % r).hexdigest()


def nonrandom_choice(s, seq):
    """
    Pick an element from ``seq`` according to the value of string ``s``.
    Guaranteed to be deterministic, tries to be uniform.
    """
    s = hashlib.md5(s).hexdigest()
    return seq[int(s, 16) % len(seq)]


def nonrandom(s, n):
    """
    Return a deterministic but pseudo-random number between 0 and ``n``.
    """
    s = hashlib.md5(s).hexdigest()
    return int(s, 16) % n


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
