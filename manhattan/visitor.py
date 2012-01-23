import logging

import time

from .util import choose_population

log = logging.getLogger(__name__)


# Goal Value Aggregation Types
RATE = 'R'
AVERAGE = 'A'
SUM = 'S'

# Goal Value Measurement Formats
NUMERIC = 'N'
CURRENCY = 'C'
PERCENTAGE = 'P'


class Visitor(object):
    """
    A handle to perform operations on the given visitor session.
    """
    def __init__(self, id, log, site_id=''):
        """
        Initialize the Visitor handle.

        :param id:
            id to reference this Visitor.
        :type id:
            str
        :param log:
            A log instance that implements the manhattan log interface methods.
        """
        self.id = id
        self.log = log
        self.site_id = str(site_id)

    def timestamp(self):
        """
        Override this to generate event timestamps in a different way. Defaults
        to the POSIX epoch.
        """
        return '%0.4f' % time.time()

    def page(self, request):
        """
        Log a page view for this visitor.

        :param request:
            A request object corresponding to the page to log.
        :type request:
            webob.Request instance
        """
        log.debug('page: %s %s', self.id, request.url)
        self.log.write(['page', self.timestamp(),
                        self.id,
                        self.site_id,
                        request.remote_addr or '0.0.0.0',
                        request.method,
                        request.url,
                        request.user_agent or '',
                        request.referer or ''])

    def pixel(self):
        """
        Log a pixel view for this visitor.
        """
        log.debug('pixel: %s', self.id)
        self.log.write(['pixel', self.timestamp(), self.id, self.site_id])

    def goal(self, name, value=None, value_type=None, value_format=None):
        """
        Log a goal hit for this visitor.

        :param name:
            Name of the goal.
        :type name:
            str
        :param value:
            Value of this goal.
        :type value:
            int or float
        :param value_type:
            Type of goal value aggregation to perform.
        :type value_type:
            RATE, AVERAGE or SUM
        :param value_format:
            Display format for this goal value.
        :type value_format:
            NUMERIC, CURRENCY, or PERCENTAGE
        """
        log.debug('goal: %s %s', self.id, name)
        self.log.write(['goal', self.timestamp(), self.id, self.site_id, name,
                        value or '', value_type or '', value_format or ''])

    def split(self, test_name, populations=None):
        """
        Perform a split test for this visitor. The resulting population is
        calculated deterministically based on the test name and the visitor id,
        so the same visitor id and the same test name will always be assigned
        to the same population.

        :param test_name:
            Name of the test.
        :type test_name:
            str
        :param populations:
            Population specified. Can be any of the following:

                None -- 50/50 split performed between True or False.
                list -- Select evenly between entries in the list.
                dict -- A weighted split between keys in the dict. The weight
                of each population is specified by the value, as a float.

        :returns:
            The population selected for the visitor.
        """
        log.debug('split: %s %s', self.id, test_name)
        selected = choose_population(self.id + test_name, populations)
        self.log.write(['split', self.timestamp(), self.id, self.site_id,
                        test_name, str(selected)])
        return selected
