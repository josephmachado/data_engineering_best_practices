from pyspark.sql import SparkSession


def create_tables(
    spark,
    path="s3a://adventureworks/delta",
    database: str = "adventureworks",
):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")

    spark.sql(f"DROP TABLE IF EXISTS {database}.sales_mart")
    spark.sql(
        f"""
              CREATE TABLE {database}.sales_mart (
                deliver_date DATE,
                state_id STRING,
                num_orders BIGINT,
                etl_inserted TIMESTAMP,
                partition STRING
                ) USING DELTA
                PARTITIONED BY (partition)
                LOCATION '{path}/sales_mart'
              """
    )


def drop_tables(spark, database: str = "adventureworks"):
    spark.sql(f"DROP TABLE IF EXISTS {database}.sales_mart")
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
