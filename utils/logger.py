import logging
import sys

FORMATTER = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')


# Spark application logger class
class Logger:
    # Base logger with SparkSession configs
    # @param spark: SparkSession
    @staticmethod
    def get(spark):
        # Spark session configs
        conf = spark.sparkContext.getConf()
        app_id = conf.get('spark.app.id')
        app_name = conf.get('spark.app.name')

        # Handler settings
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(FORMATTER)

        # Setting logger
        logger = logging.getLogger('app_id/app_name' % {
            'app_id': app_id,
            'app_name': app_name
        })
        logger.addHandler(handler)

        return logger
