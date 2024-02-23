import random
import uuid
from datetime import datetime
from typing import List, Tuple

from faker import Faker
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (IntegerType, StringType, StructField,
                               StructType, TimestampType)

########################################################################
# GENERATING FAKE BRONZE DATA !!!
########################################################################


def _get_orders(
    cust_ids: List[int], num_orders: int
) -> List[Tuple[str, int, str, str, datetime, datetime]]:
    items = [
        "chair",
        "car",
        "toy",
    ]
    return [
        (
            str(uuid.uuid4()),
            int(random.choice(cust_ids)),
            str(uuid.uuid4()),
            random.choice(items),
            datetime.now(),
            datetime.now(),
        )
        for _ in range(num_orders)
    ]


def _get_customer_data(
    cust_ids: List[int],
) -> List[Tuple[int, str, str, str, datetime, datetime]]:
    fake = Faker()
    return [
        (
            cust_id,
            fake.first_name(),
            fake.last_name(),
            fake.state_abbr(),
            datetime.now(),
            datetime.now(),
        )
        for cust_id in cust_ids
    ]


def generate_bronze_data(
    spark: SparkSession,
    iteration: int = 1,
    orders_bucket: str = "app-orders",
    **kwargs,
) -> List[DataFrame]:
    cust_ids = [i for i in range(1000)]
    return [
        spark.createDataFrame(
            data=_get_customer_data(cust_ids),
            schema=StructType(
                [
                    StructField("id", IntegerType(), True),
                    StructField("first_name", StringType(), True),
                    StructField("last_name", StringType(), True),
                    StructField("state_id", StringType(), True),
                    StructField("datetime_created", TimestampType(), True),
                    StructField("datetime_updated", TimestampType(), True),
                ]
            ),
        ),
        spark.createDataFrame(
            data=_get_orders(cust_ids, 10000),
            schema=StructType(
                [
                    StructField("order_id", StringType(), True),
                    StructField("customer_id", IntegerType(), True),
                    StructField("item_id", StringType(), True),
                    StructField("item_name", StringType(), True),
                    StructField("delivered_on", TimestampType(), True),
                    StructField(
                        "datetime_order_placed", TimestampType(), True
                    ),
                ]
            ),
        ),
    ]


########################################################################
