import unittest
from tests.pyspark_test import PySparkTest


class TestLoaders(PySparkTest):

    @unittest.skip('Requires Hive support')
    def test_overwrite_table(self):
        pass

    @unittest.skip('Requires Hive support')
    def test_insert_table(self):
        pass
