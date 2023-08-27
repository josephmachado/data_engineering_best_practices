from pyspark.sql import SparkSession


def create_tables(spark, path="s3a://adventureworks/delta"):
    spark.sql("CREATE DATABASE IF NOT EXISTS adventureworks")

    spark.sql("DROP TABLE IF EXISTS adventureworks.sales_mart")
    spark.sql(
        f"""
              CREATE TABLE adventureworks.sales_mart (
                deliver_date STRING,
                state_id STRING,
                num_orders BIGINT,
                etl_inserted TIMESTAMP
                ) USING DELTA
                LOCATION '{path}/sales_mart'
              """
    )


def drop_tables(spark):
    spark.sql("DROP TABLE IF EXISTS adventureworks.sales_mart")
    spark.sql("DROP DATABASE IF EXISTS adventureworks")


if __name__ == '__main__':
    spark = (
        SparkSession.builder.appName("adventureworks_ddl")
        .config("spark.executor.cores", "1")
        .config("spark.executor.instances", "1")
        .enableHiveSupport()
        .getOrCreate()
    )
    create_tables(spark)
