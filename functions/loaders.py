# Dataframe loaders
class Loaders:
    # Write DataFrame by overwrite
    # @param df: source DataFrame
    # @param table_name: target table name
    @staticmethod
    def overwrite_table(df, table_name):
        return df \
            .write \
            .mode('overwrite') \
            .format('parquet') \
            .insertInto(f"{table_name}")

    # Write DataFrame by insert
    # @param df: source DataFrame
    # @param table_name: target table name
    @staticmethod
    def insert_table(df, table_name):
        return df \
            .write \
            .mode('append') \
            .format('parquet') \
            .insertInto(f"{table_name}")
