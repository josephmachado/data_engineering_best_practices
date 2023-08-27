from delta import *
from pyspark.sql import SparkSession


def create_tables(spark, path="s3a://adventureworks/delta"):
    spark.sql("CREATE DATABASE IF NOT EXISTS adventureworks")

    spark.sql("DROP TABLE IF EXISTS adventureworks.customer")
    spark.sql(
        f"""
              CREATE TABLE adventureworks.customer (
                id INT,
                first_name STRING,
                last_name STRING,
                state_id STRING,
                datetime_created TIMESTAMP,
                datetime_updated TIMESTAMP,
                etl_inserted TIMESTAMP
                ) USING DELTA
                LOCATION '{path}/customer'
              """
    )

    spark.sql("DROP TABLE IF EXISTS adventureworks.orders")
    spark.sql(
        f"""
              CREATE TABLE adventureworks.orders (
                order_id STRING,
                customer_id INT,
                item_id STRING,
                item_name STRING,
                delivered_on TIMESTAMP,
                datetime_order_placed TIMESTAMP,
                etl_inserted TIMESTAMP
                ) USING DELTA
                LOCATION '{path}/orders'
              """
    )


def drop_tables(spark):
    spark.sql("DROP TABLE IF EXISTS adventureworks.customer")
    spark.sql("DROP TABLE IF EXISTS adventureworks.orders")
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
