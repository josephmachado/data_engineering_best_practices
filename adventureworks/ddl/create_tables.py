from create_bronze_tables import create_tables as create_bronze
from create_gold_tables import create_tables as create_gold
from create_interface_views import create_tables as create_interface
from create_silver_tables import create_tables as create_silver
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("{database}_ddl")
        .config("spark.executor.cores", "1")
        .config("spark.executor.instances", "1")
        .enableHiveSupport()
        .getOrCreate()
    )
    create_bronze(spark)
    create_silver(spark)
    create_gold(spark)
    create_interface(spark)
