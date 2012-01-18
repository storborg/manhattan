from time import time
from webob import Request

from manhattan.visitor import Visitor
from manhattan.util import nonce
from manhattan.log.gz import GZEventLog
from manhattan.log.memory import MemoryLog


def run_logger(log, num_requests=10000, goal_every=50, split_every=1):
    """
    Test the frontend performance by simulating a stream of requests.

    Tweak the following parameters:

    :param goal_every:
        Record a goal hit every ``goal_every`` requests.

    :param split_every:
        Record a split every ``split_every`` requests.
    """
    vid = nonce()
    req = Request.blank('/foo/bar')

    print "Logging %d requests." % num_requests
    start = time()
    for ii in xrange(num_requests):
        vis = Visitor(vid, log)
        vis.page(req)
        if split_every and (ii % split_every) == 0:
            vis.split('fake test')
        if goal_every and (ii % goal_every) == 0:
            vis.goal('fake goal')
    end = time()
    elapsed = end - start
    throughput = num_requests / elapsed
    print "Handled %0.2f req / sec" % throughput

    count = len(list(log.process()))
    print "Recorded %d records." % count


if __name__ == '__main__':
    print "Testing MemoryLog"
    run_logger(MemoryLog())
    print "Testing GZEventLog"
    run_logger(GZEventLog('/tmp/manhattan-perftest'))
