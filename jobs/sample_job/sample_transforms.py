from functions.base_transforms import BaseTransforms
from functions.loaders import Loaders
from pyspark.sql.functions import *


# Transforms of SampleJob
class SampleTransforms(BaseTransforms):
    # @param spark: SparkSession
    def __init__(self, __spark):
        super().__init__(__spark)

    # Add column with last day of report_dt
    # @param df: source DataFrame
    # @return : target DataFrame
    @staticmethod
    def with_month_end(df):
        return df.withColumn("month_end_report_dt", last_day(col("report_dt")))

    # Filter DataFrame by report_dt
    # @param df: source DataFrame
    # @param from_date: begin of report period
    # @param to_date: end of report period
    # @return : target DataFrame
    @staticmethod
    def where_report_dt(df, from_date, to_date):
        return df.where(f"report_dt between '{from_date}' and '{to_date}'")

    # Join with car table
    # @param df: source DataFrame
    # @param join_table: join car table name
    # @return : target DataFrame */
    def join_car(self, df, join_table):
        df.createOrReplaceTempView("df")
        return self.__spark.sql(
            f"""
                    select
                        a.customer_id,
                        a.customer_nm,
                        b.car_nm
                    from df as a
                    left join {join_table} as b
                        on a.customer_id = b.customer_id
                    """)

    # Load target table
    # @param df: Source DataFrame
    # @param target_name: target table name
    # @return : target DataFrame
    @staticmethod
    def load(df, target_name):
        return Loaders.overwrite_table(df, target_name)
