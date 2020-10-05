import sys
from abc import ABC
from utils.main_job import MainJob
from jobs.sample_job.sample_transforms import SampleTransforms


# Sample job ETL process
class SampleJob(MainJob, ABC):
    # Main job function
    def run(self):
        # SparkSession, logger, parameters, environment and job transforms
        spark = self.spark
        logger = self.logger
        params = self.params
        env = self.env

        # Schemas
        source_schema = env['source_schema']
        target_schema = env['target_schema']

        # Hive sources
        customer_table = spark.sparkContext.broadcast(f'{source_schema}.customer')
        car_table = spark.sparkContext.broadcast(f'{source_schema}.car')

        # Hive target
        dm_table = spark.sparkContext.broadcast(f'{target_schema}.dm_table')

        # Report dates
        report_begin_date = params.get('report_begin_date')
        report_end_date = params.get('report_end_date')
        logger.info('Report begin date: %(rbd)s' % {'rbd': report_begin_date})
        logger.info('Report end date: %(red)s' % {'red': report_end_date})

        # Transformations
        logger.info('Transformations begin')
        st = SampleTransforms(spark)

        st.read_source_table(customer_table)\
            .transform(st.with_month_end)\
            .transform(lambda df: st.where_report_dt(df, report_begin_date, report_end_date))\
            .transform(lambda df: st.join_car(df, car_table))\
            .transform(st.with_processed_dttm)\
            .transform(lambda df: st.repartition_by_type(df, 'plan'))\
            .transform(lambda df: st.load(df, dm_table))

        logger.info('Transformations end')

        spark.stop()


def main(args):
    SampleJob(args).run()


if __name__ == '__main__':
    main(sys.argv[1:])
