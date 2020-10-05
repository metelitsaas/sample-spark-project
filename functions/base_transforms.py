from functions.spark_functions import SparkFunctions
from pyspark.sql.functions import *


# Basic transformations
class BaseTransforms:
    # @param spark: SparkSession
    def __init__(self, spark):
        self.__spark = spark

    # Read source table
    # @param table_name: table name
    # @return : target DataFrame
    def read_source_table(self, table_name):
        return self.__spark.table(table_name)

    # Add processed timestamp columns
    # @param df: source DataFrame
    # @return : target DataFrame
    @staticmethod
    def with_processed_dttm(df):
        return df.withColumn('processed_dttm', current_timestamp())

    # Cast DataFrame timestamp type fields to string
    # @param df: source DataFrame
    # @return : target DataFrame
    @staticmethod
    def cast_timestamp_to_string(df):
        pass  # TODO: Add cast_timestamp_to_string function

    # Broadcast long tail of DataFrame to resolve data skew
    # @param df: source DataFrame
    # @param columns: sequence of Columns with skew
    # @param sigma: sigma value
    # @return : target DataFrame
    @staticmethod
    def broadcast_frequent_skew(df, columns, sigma=2):
        pass  # TODO: Add broadcast_frequent_skew function

    # Calculate partition by object size and HDFS block size
    # @param rep_type: two types of size calculation:
    #   "folder" - by folder size in HDFS
    #   "plan" - by DataFrame's catalyst plan
    # @param df: source DataFrame
    # @return : target DataFrame
    def repartition_by_type(self, df, rep_type='folder'):
        # Choose type of DataFrame size calculation
        df_size = {
            'folder': SparkFunctions.df_size_by_folder(self.__spark, df),
            'plan': SparkFunctions.df_size_by_plan(self.__spark, df)
        }.get(rep_type)

        # HDFS block size
        block_size = SparkFunctions.get_hdfs_block_size(self.__spark)

        # Number of partition based on DataFrame size and HDFS block size
        num = SparkFunctions.partitions_count(block_size, df_size)

        return df.repartition(num)
