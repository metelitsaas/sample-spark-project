from pyspark.sql import SparkSession
from utils.logger import Logger

DYNAMIC_PARTITION_MODE = 'nonstrict'
PARTITION_OVERWRITE_MODE = 'dynamic'
SPARK_TIMEZONE = 'UTC'


#  Spark session configuration class
class SparkSessionConfig:
    # @param app_name: name of Spark application
    # @param master: master URL of Spark cluster
    def __init__(self, app_name, master):
        self.app_name = app_name
        self.master = master
        self.spark = SparkSession \
            .builder \
            .appName(app_name) \
            .master(master) \
            .config('hive.exec.dynamic.partition.mode', DYNAMIC_PARTITION_MODE) \
            .config('spark.sql.sources.partitionOverwriteMode', PARTITION_OVERWRITE_MODE) \
            .config('spark.sql.session.timeZone', SPARK_TIMEZONE) \
            .config('spark.sql.catalogImplementation', self.catalog_type(self.master)) \
            .getOrCreate()

        self.logger = Logger.get(self.spark)

    # Get catalog type based on master type: if local then in-memory, if cluster then hive metastore
    # Only for unit testing support
    # @return : catalog implementation type
    @staticmethod
    def catalog_type(master):
        return 'hive' if master != 'local' else 'in-memory'
