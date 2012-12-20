import logging

from decimal import Decimal

from sqlalchemy import MetaData, create_engine
from sqlalchemy.exc import SAWarning

import warnings
# Filter out pysqlite Decimal loss of precision warning.
warnings.filterwarnings('ignore',
                        '.*pysqlite does \*not\* support Decimal',
                        SAWarning,
                        'sqlalchemy.types')
# Turn unicode bind param warnings into errors.
warnings.filterwarnings('error',
                        r'Unicode type received non-unicode bind',
                        SAWarning,
                        'sqlalchemy.engine.default')

from manhattan.worker import Worker

from manhattan.log.memory import MemoryLog
from manhattan.log.timerotating import TimeRotatingLog

from manhattan.backend import Backend

from . import data
from .base import BaseTest, work_path

logging.basicConfig()
logging.getLogger('sqlalchemy.engine').setLevel(logging.DEBUG)


def drop_existing_tables(engine):
    "Drop all tables, including tables that aren't defined in metadata."
    temp_metadata = MetaData()
    temp_metadata.reflect(bind=engine)
    for table in reversed(temp_metadata.sorted_tables):
        table.drop(bind=engine)


class TestCombinations(BaseTest):

    def _check_backend_queries(self, backend):
        self.assertEqual(backend.count(u'add to cart', site_id=1), 5)
        self.assertEqual(backend.count(u'began checkout', site_id=1), 4)
        self.assertEqual(backend.count(u'viewed page', site_id=1), 6)

        self.assertEqual(backend.count(u'add to cart', site_id=2), 1)
        self.assertEqual(backend.count(u'began checkout', site_id=2), 1)
        self.assertEqual(backend.count(u'completed checkout', site_id=2), 0)
        self.assertEqual(backend.count(u'viewed page', site_id=2), 1)

        num = backend.count(u'completed checkout',
                            variant=(u'red checkout form', u'False'),
                            site_id=1)
        self.assertEqual(num, 3)

        revenue = backend.goal_value(u'completed checkout', site_id=1)
        self.assertEqual(revenue, Decimal('108.19'))

        revenue_nored = backend.goal_value(
            u'completed checkout',
            variant=(u'red checkout form', u'False'), site_id=1)
        self.assertEqual(revenue_nored, Decimal('108.19'))

        noreds = backend.count(variant=(u'red checkout form', u'False'),
                               site_id=1)
        self.assertEqual(noreds, 3)

        margin = backend.goal_value(u'order margin', site_id=1)
        margin = margin.quantize(Decimal('.01'))
        self.assertEqual(margin, Decimal('23.47'))

        margin_per = backend.goal_value(u'margin per session', site_id=1)
        margin_per = margin_per.quantize(Decimal('.01'))
        self.assertEqual(margin_per, Decimal('3.90'))

        margin_per_noreds = backend.goal_value(
            u'margin per session',
            variant=(u'red checkout form', u'False'), site_id=1)
        margin_per_noreds = margin_per_noreds.quantize(Decimal('.01'))
        self.assertEqual(margin_per_noreds, Decimal('7.79'))

        abandoned_carts = backend.count(u'abandoned cart', site_id=1)
        self.assertEqual(abandoned_carts, 1)

        abandoned_checkouts = backend.count(u'abandoned checkout', site_id=1)
        self.assertEqual(abandoned_checkouts, 1)

        abandoned_payment = backend.count(u'abandoned after payment failure',
                                          site_id=1)
        self.assertEqual(abandoned_payment, 1)

        abandoned_validation = backend.count(
            u'abandoned after validation failure', site_id=1)
        self.assertEqual(abandoned_validation, 0)

        self.assertEqual(backend.all_tests(),
                         [('red checkout form', 1602, 7246)])

        self.assertEqual(backend.results('red checkout form',
                                         ['viewed page',
                                          'add to cart',
                                          'began checkout',
                                          'completed checkout'],
                                         site_id=1),
                         {
                             u'True': [1, 0, 1, Decimal('0')],
                             u'False': [3, 0, 3, Decimal('108.19')]
                         })

    def _get_backend(self, reset=False):
        #url = 'mysql://manhattan:quux@localhost/manhattan_test'
        url = 'sqlite:////tmp/manhattan-test.db'
        if reset:
            drop_existing_tables(create_engine(url))
        return Backend(url, flush_every=2, cache_size=5,
                       complex_goals=data.test_complex_goals)

    def test_resume(self):
        path = work_path('resume')

        backend = self._get_backend(reset=True)

        log_w = TimeRotatingLog(path)
        data.run_clickstream(log_w, first=0, last=25)

        log_r1 = TimeRotatingLog(path)
        worker1 = Worker(log_r1, backend)
        worker1.run()

        first_pointer = backend.get_pointer()
        self.assertIsNotNone(first_pointer)

        backend = self._get_backend(reset=False)

        data.run_clickstream(log_w, first=25)
        log_r2 = TimeRotatingLog(path)
        worker2 = Worker(log_r2, backend)
        worker2.run(resume=True)

        second_pointer = backend.get_pointer()
        self.assertIsNotNone(second_pointer)

        self._check_backend_queries(backend)

    def test_basic(self):
        path = work_path('basic')

        backend = self._get_backend(reset=True)

        log_w = TimeRotatingLog(path)
        data.run_clickstream(log_w)

        log_r = TimeRotatingLog(path)
        worker1 = Worker(log_r, backend)
        worker1.run()

        self._check_backend_queries(backend)

    def test_memory_log(self):
        log = MemoryLog()
        data.run_clickstream(log)

        backend = self._get_backend(reset=True)

        worker1 = Worker(log, backend)
        worker1.run(resume=False)

        self._check_backend_queries(backend)
