import os.path
import shutil
from unittest import TestCase


work_dir = '/tmp/manhattan-tests'


def work_path(path):
    return os.path.join(work_dir, path)


class BaseTest(TestCase):

    def setUp(self):
        if os.path.exists(work_dir):
            shutil.rmtree(work_dir)
