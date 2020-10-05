import unittest
from utils.spark_session_config import SparkSessionConfig


class TestSparkSessionConfig(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        master = 'local'
        cls.spark_session_config = SparkSessionConfig('TestSparkSessionConfig', master)
        cls.spark = cls.spark_session_config.spark
        cls.logger = cls.spark_session_config.logger

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_initialize_spark(self):
        tmp_value = self.spark\
            .sql('select count(*)')\
            .head()[0]

        self.assertEqual(1, tmp_value)

    def test_configs(self):
        app_name = self.spark.conf.get("spark.app.name")
        dynamic_partition = self.spark.conf.get("hive.exec.dynamic.partition.mode")
        partition_overwrite = self.spark.conf.get("spark.sql.sources.partitionOverwriteMode")
        spark_timezone = self.spark.conf.get("spark.sql.session.timeZone")
        hive_support = self.spark.conf.get("spark.sql.catalogImplementation")

        self.assertEqual("TestSparkSessionConfig", app_name)
        self.assertEqual("nonstrict", dynamic_partition)
        self.assertEqual("dynamic", partition_overwrite)
        self.assertEqual("UTC", spark_timezone)
        self.assertEqual("in-memory", hive_support)

    def test_catalog_type(self):
        test_local = self.spark_session_config.catalog_type('local')
        test_other = self.spark_session_config.catalog_type('yarn')

        self.assertEqual('in-memory', test_local)
        self.assertEqual('hive', test_other)
