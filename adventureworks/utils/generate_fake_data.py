import random
import time
import uuid
from contextlib import contextmanager
from datetime import datetime
from typing import Any, List, Tuple

from delta.tables import *
from faker import Faker
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import *

STATES_LIST = [
    "AC",
    "AL",
    "AP",
    "AM",
    "BA",
    "CE",
    "DF",
    "ES",
    "GO",
    "MA",
    "MT",
    "MS",
    "MG",
    "PA",
    "PB",
    "PR",
    "PE",
    "PI",
    "RJ",
    "RN",
    "RS",
    "RO",
    "RR",
    "SC",
    "SP",
    "SE",
    "TO",
]


def _get_orders(
    cust_ids: List[int], num_orders: int
) -> List[Tuple[str, int, str, str, str, str]]:
    # order_id, customer_id, item_id, item_name, delivered_on
    items = [
        "chair",
        "car",
        "toy",
        "laptop",
        "box",
        "food",
        "shirt",
        "weights",
        "bags",
        "carts",
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
) -> List[Tuple[int, str, str, str, str, str]]:
    fake = Faker()
    return [
        (
            cust_id,
            fake.first_name(),
            fake.last_name(),
            random.choice(STATES_LIST),
            datetime.now(),
            datetime.now(),
        )
        for cust_id in cust_ids
    ]


def generate_bronze_data(
    spark: SparkSession,
    iteration: int = 1,
    orders_bucket: str = "app-orders",
    **kwargs
) -> List[DataFrame]:
    cust_ids = [i for i in range(1000)]
    orders_data = _get_orders(cust_ids, 10000)
    customer_data = _get_customer_data(cust_ids)
    customer_cols = [
        "id",
        "first_name",
        "last_name",
        "state_id",
        "datetime_created",
        "datetime_updated",
    ]
    customer_schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("first_name", StringType(), True),
            StructField("last_name", StringType(), True),
            StructField("state_id", StringType(), True),
            StructField("datetime_created", TimestampType(), True),
            StructField("datetime_updated", TimestampType(), True),
        ]
    )
    customer_df = spark.createDataFrame(
        data=customer_data, schema=customer_schema
    )
    orders_cols = [
        "order_id",
        "customer_id",
        "item_id",
        "item_name",
        "delivered_on",
        "datetime_order_placed",
    ]
    orders_schema = StructType(
        [
            StructField("order_id", StringType(), True),
            StructField("customer_id", IntegerType(), True),
            StructField("item_id", StringType(), True),
            StructField("item_name", StringType(), True),
            StructField("delivered_on", TimestampType(), True),
            StructField("datetime_order_placed", TimestampType(), True),
        ]
    )
    orders_df = spark.createDataFrame(data=orders_data, schema=orders_schema)
    return [customer_df, orders_df]
