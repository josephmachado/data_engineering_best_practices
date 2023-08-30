from pyspark.sql import SparkSession


def create_tables(
    spark,
    path="s3a://adventureworks/delta",
    database: str = "adventureworks",
):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")

    spark.sql(f"DROP TABLE IF EXISTS {database}.customer")
    spark.sql(
        f"""
              CREATE TABLE {database}.customer (
                id INT,
                first_name STRING,
                last_name STRING,
                state_id STRING,
                datetime_created TIMESTAMP,
                datetime_updated TIMESTAMP,
                etl_inserted TIMESTAMP,
                partition STRING
                ) USING DELTA
                PARTITIONED BY (partition)
                LOCATION '{path}/customer'
              """
    )

    spark.sql(f"DROP TABLE IF EXISTS {database}.orders")
    spark.sql(
        f"""
              CREATE TABLE {database}.orders (
                order_id STRING,
                customer_id INT,
                item_id STRING,
                item_name STRING,
                delivered_on TIMESTAMP,
                datetime_order_placed TIMESTAMP,
                etl_inserted TIMESTAMP,
                partition STRING
                ) USING DELTA
                PARTITIONED BY (partition)
                LOCATION '{path}/orders'
              """
    )


def drop_tables(
    spark,
    database: str = "adventureworks",
):
    spark.sql(f"DROP TABLE IF EXISTS {database}.customer")
    spark.sql(f"DROP TABLE IF EXISTS {database}.orders")
    spark.sql(f"DROP DATABASE IF EXISTS {database}")


if __name__ == '__main__':
    spark = (
        SparkSession.builder.appName("{database}_ddl")
        .config("spark.executor.cores", "1")
        .config("spark.executor.instances", "1")
        .enableHiveSupport()
        .getOrCreate()
    )
    create_tables(spark)
