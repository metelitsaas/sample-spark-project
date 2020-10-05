import math


# Spark functions class
class SparkFunctions:
    # Get HDFS block size in bytes
    # @param spark: SparkSession
    # @return : hdfs block size
    @staticmethod
    def get_hdfs_block_size(spark):
        return int(spark.sparkContext._jsc.hadoopConfiguration().get('dfs.block.size'))

    @staticmethod
    # Get Hive table location
    # @param spark: SparkSession
    # @param df: source DataFrame
    # @return : path of Hive table folder
    def hive_table_location(spark, df):
        df.createOrReplaceTempView('df')

        return spark.sql('desc formatted df') \
            .filter("col_name == 'Location'") \
            .head(1)[0][1]

    @staticmethod
    # Get HDFS folder size by path
    # @param spark: SparkSession
    # @param path: path of folder
    # @return : Folder size in bytes */
    def folder_size(spark, path):
        conf = spark.sparkContext._jsc.hadoopConfiguration()
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(conf)
        hdfs_path = spark._jvm.org.apache.hadoop.fs.Path(path)

        return fs.getContentSummary(hdfs_path).getLength()

    @staticmethod
    # Calculate partitions by object size and HDFS block size
    # @param block_size: size of HDFS block in bytes
    # @param df_size: size of DataFrame in bytes
    # @return : estimated count of partitions
    def partitions_count(block_size, df_size):
        return math.ceil(df_size / block_size)

    @staticmethod
    # Size of DataFrame by folder size by optimized plan
    # @param spark: SparkSession
    # @param df: source DataFrame
    # @return : DataFrame size in bytes
    def df_size_by_plan(spark, df):
        catalyst_plan = df._jdf.logicalPlan()
        df_size = spark\
            ._jsparkSession\
            .sessionState()\
            .executePlan(catalyst_plan)\
            .optimizedPlan()\
            .stats()\
            .sizeInBytes()

        return df_size

    @staticmethod
    # Size of DataFrame by folder size
    # @param spark: SparkSession
    # @param df: source DataFrame
    # @return : DataFrame size in bytes
    def df_size_by_folder(spark, df):
        catalyst_plan = df._jdf.logicalPlan()
        df_size = spark \
            ._jsparkSession \
            .sessionState() \
            .executePlan(catalyst_plan) \
            .optimizedPlan() \
            .stats() \
            .sizeInBytes()

        return df_size
