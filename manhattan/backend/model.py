class VisitorHistory(object):

    def __init__(self):
        self.nonbot = False
        self.nonbot_queue = []
        self.goals = set()
        self.variants = set()
        self.ips = set()
        self.user_agents = set()

        # Set of (goal name, rollup key, bucket start) that have already been
        # counted.
        self.conversion_keys = set()

        # Set of (test name, selected, rollup key, bucket start) that have
        # already been counted.
        self.impression_keys = set()

        # Set of (test name, selected, goal name, rollup key, bucket start)
        # that have already been counted.
        self.variant_conversion_keys = set()

        # Dict mapping complex goal name to a list of previous conversion keys
        # on that complex goal.
        self.complex_keys = {}


class Test(object):

    def __init__(self, first_timestamp=None, last_timestamp=None):
        self.first_timestamp = first_timestamp
        self.last_timestamp = last_timestamp


class Goal(object):

    def __init__(self, value_type=None, value_format=None):
        self.value_type = value_type
        self.value_format = value_format
