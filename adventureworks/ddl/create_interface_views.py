from pyspark.sql import SparkSession


def create_tables(
    spark,
    database: str = "businessintelligence",
    src_database: str = "adventureworks",
):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
    spark.sql(
        f"""
              CREATE OR REPLACE VIEW {database}.sales_mart
              AS
              SELECT state_id
              , deliver_date as delivery_date
              , num_orders
              FROM {src_database}.sales_mart
              WHERE partition in
              (SELECT max(partition) FROM {src_database}.sales_mart)
              """
    )


def drop_tables(spark, database: str = "businessintelligence"):
    spark.sql(f"DROP VIEW IF EXISTS {database}.sales_mart")
    spark.sql(f"DROP DATABASE IF EXISTS {database}")


if __name__ == '__main__':
    spark = (
        SparkSession.builder.appName("adventureworks_ddl")
        .config("spark.executor.cores", "1")
        .config("spark.executor.instances", "1")
        .enableHiveSupport()
        .getOrCreate()
    )
    create_tables(spark)
