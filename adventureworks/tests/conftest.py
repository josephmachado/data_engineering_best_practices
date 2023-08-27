import pyspark
import pytest
from delta import configure_spark_with_delta_pip


@pytest.fixture(scope='session')
def spark():
    my_packages = [
        "io.delta:delta-core_2.12:2.3.0",
        "org.apache.hadoop:hadoop-aws:3.3.2",
    ]

    builder = (
        pyspark.sql.SparkSession.builder.appName("adventureworks_tests")
        .config("spark.executor.cores", "1")
        .config("spark.executor.instances", "1")
        .config("spark.sql.shuffle.partitions", "1")
        .config(
            "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .enableHiveSupport()
    )

    spark = configure_spark_with_delta_pip(
        builder, extra_packages=my_packages
    ).getOrCreate()

    yield spark
    spark.stop
