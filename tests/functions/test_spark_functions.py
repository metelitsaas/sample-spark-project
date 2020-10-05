import unittest
from tests.pyspark_test import PySparkTest
from functions.spark_functions import SparkFunctions as sf


class TestSparkFunctions(PySparkTest):

    @unittest.skip
    def test_get_hdfs_block_size(self):
        pass

    @unittest.skip
    def test_hive_table_location(self):
        pass

    @unittest.skip
    def test_folder_size(self):
        pass

    def test_partitions_count(self):
        self.assertEqual(1, sf.partitions_count(8192, 8192))
        self.assertEqual(2, sf.partitions_count(8192, 9000))
        self.assertEqual(1, sf.partitions_count(8192, 6000))
        self.assertEqual(2, sf.partitions_count(8192, 16000))
        self.assertEqual(3, sf.partitions_count(8192, 20000))

    @unittest.skip
    def test_df_size_by_plan(self):
        pass

    @unittest.skip
    def test_df_size_by_folder(self):
        pass
