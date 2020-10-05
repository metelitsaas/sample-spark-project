from tests.pyspark_test import PySparkTest
from utils.logger import Logger


class TestLogger(PySparkTest):

    def test_get(self):
        logger = Logger.get(self.spark)

        logger.info('Info message')
        logger.warn('Warning message')
