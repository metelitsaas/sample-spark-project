import unittest
from tests.pyspark_test import PySparkTest
from functions.base_transforms import BaseTransforms
from pyspark.sql.types import IntegerType, TimestampType


class TestBaseTransforms(PySparkTest):

    def test_read_source_table(self):
        data = range(1, 10)
        len_data = len(data)

        src_df = self.spark.createDataFrame(data, IntegerType())

        t = BaseTransforms(self.spark)
        trg_df = t.read_source_table('src_df')

        self.assertEqual(src_df.take(len_data), trg_df.take(len_data))

    def test_with_processed_dttm(self):
        df = self.spark.createDataFrame([1, 2, 3], IntegerType())
        t = BaseTransforms(self.spark)
        trg_df = t.with_processed_dttm(df)

        column_type = [f.dataType for f in trg_df.schema.fields if f.name == 'processed_dttm'][0]

        self.assertEqual(TimestampType, type(column_type))

    @unittest.skip
    def test_cast_timestamp_to_string(self):
        pass

    @unittest.skip
    def test_broadcast_frequent_skew(self):
        pass

    @unittest.skip
    def test_repartition_by_type(self):
        pass
