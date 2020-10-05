import unittest
import logging
from pyspark.sql import SparkSession


# Base PySpark testing class
class PySparkTest(unittest.TestCase):

    @classmethod
    def logging(cls):
        logger = logging.getLogger(cls.__name__)
        logger.setLevel(logging.WARN)

    @classmethod
    def create_test_spark_session(cls):
        return SparkSession\
            .builder\
            .master('local')\
            .appName('test_spark_session')\
            .getOrCreate()

    @classmethod
    def setUpClass(cls):
        cls.logging()
        cls.spark = cls.create_test_spark_session()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
