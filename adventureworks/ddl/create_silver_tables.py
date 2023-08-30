from pyspark.sql import SparkSession


def create_tables(
    spark,
    path: str = "s3a://adventureworks/delta",
    database: str = "adventureworks",
):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")

    spark.sql(f"DROP TABLE IF EXISTS {database}.dim_customer")
    spark.sql(
        f"""
              CREATE TABLE {database}.dim_customer (
                id INT,
                customer_sur_id STRING,
                first_name STRING,
                last_name STRING,
                state_id STRING,
                datetime_created TIMESTAMP,
                datetime_updated TIMESTAMP,
                current boolean,
                valid_from TIMESTAMP,
                valid_to TIMESTAMP,
                partition STRING
                ) USING DELTA
                LOCATION '{path}/dim_customer'
              """
    )

    spark.sql(f"DROP TABLE IF EXISTS {database}.fct_orders")
    spark.sql(
        f"""
              CREATE TABLE {database}.fct_orders (
                order_id STRING,
                customer_id INT,
                item_id STRING,
                item_name STRING,
                delivered_on TIMESTAMP,
                datetime_order_placed TIMESTAMP,
                customer_sur_id STRING,
                etl_inserted TIMESTAMP,
                partition STRING
                ) USING DELTA
                PARTITIONED BY (partition)
                LOCATION '{path}/fct_orders'
              """
    )


def drop_tables(
    spark,
    database: str = "adventureworks",
):
    spark.sql(f"DROP TABLE IF EXISTS {database}.dim_customer")
    spark.sql(f"DROP TABLE IF EXISTS {database}.fct_orders")
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
