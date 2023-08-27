import logging
import os
import random
import time
import uuid
from abc import ABC, abstractmethod
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any, Dict, List, Tuple

import great_expectations as gx
from delta.tables import *
from faker import Faker
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import lit
from pyspark.sql.types import *
from pyspark.sql.types import StringType


@dataclass
class DeltaDataSet:
    name: str
    curr_data: DataFrame
    primary_keys: List[str]
    storage_path: str
    table_name: str
    data_type: str
    schema: str
    partition: str
    skip_publish: bool = False


class InValidDataException(Exception):
    pass


class StandardETL(ABC):
    STORAGE_PATH = 's3a://adventureworks/delta'
    SCHEMA = 'adventureworks'

    def run_data_validations(self, input_datasets: Dict[str, DeltaDataSet]):
        context = gx.get_context(
            context_root_dir=os.path.join(
                os.getcwd(),
                "adventureworks",  # make this changeable
                "great_expectations",  # make this changeable
            )
        )

        validations = []
        for input_dataset in input_datasets.values():
            validations.append(
                {
                    "batch_request": context.get_datasource("spark_datasource")
                    .get_asset(input_dataset.name)
                    .build_batch_request(dataframe=input_dataset.curr_data),
                    "expectation_suite_name": input_dataset.name,
                }
            )
        return context.run_checkpoint(
            checkpoint_name="dq_checkpoint", validations=validations
        ).list_validation_results()

    def validate_data(self, input_datasets: Dict[str, DeltaDataSet]) -> bool:
        results = {}
        for validation in self.run_data_validations(input_datasets):
            results[
                validation.get('meta').get('expectation_suite_name')
            ] = validation.get('success')
        for k, v in results.items():
            if not v:
                raise InValidDataException(
                    f"The {k} dataset did not pass validation, please check"
                    " the metadata db for more information"
                )

        return True

    def construct_join_string(self, keys: List[str]) -> str:
        return ' AND '.join([f"target.{key} = source.{key}" for key in keys])

    def publish_data(
        self, input_datasets: Dict[str, DeltaDataSet], spark, **kwargs
    ) -> None:
        for input_dataset in input_datasets.values():
            if not input_dataset.skip_publish:
                targetDF = DeltaTable.forPath(
                    spark, input_dataset.storage_path
                )
                # todo: use delta table generated column, when its available in create DDL
                input_dataset.curr_data = input_dataset.curr_data.withColumn(
                    'etl_inserted', current_timestamp()
                )
                (
                    targetDF.alias("target")
                    .merge(
                        input_dataset.curr_data.alias("source"),
                        self.construct_join_string(input_dataset.primary_keys),
                    )
                    .whenMatchedUpdateAll()
                    .whenNotMatchedInsertAll()
                    .whenNotMatchedBySourceDelete()
                    .execute()
                )

    @abstractmethod
    def get_bronze_datasets(self, spark, **kwargs) -> Dict[str, DeltaDataSet]:
        pass

    @abstractmethod
    def get_silver_datasets(
        self, input_datasets: List[DeltaDataSet], spark, **kwargs
    ) -> Dict[str, DeltaDataSet]:
        pass

    @abstractmethod
    def get_gold_datasets(
        self, input_datasets: List[DeltaDataSet], spark, **kwargs
    ) -> Dict[str, DeltaDataSet]:
        pass

    def run(self, spark, **kwargs):
        partition = kwargs.get('partition')
        bronze_data_sets = self.get_bronze_datasets(spark, partition=partition)
        self.validate_data(bronze_data_sets)
        self.publish_data(bronze_data_sets, spark)
        logging.info(
            'Created, validated & published bronze datasets:'
            f' {[ds for ds in bronze_data_sets.keys()]}'
        )

        silver_data_sets = self.get_silver_datasets(
            bronze_data_sets, spark, partition=partition
        )
        self.validate_data(silver_data_sets)
        self.publish_data(silver_data_sets, spark)
        logging.info(
            'Created, validated & published silver datasets:'
            f' {[ds for ds in silver_data_sets.keys()]}'
        )

        gold_data_sets = self.get_gold_datasets(
            silver_data_sets, spark, partition=partition
        )
        self.validate_data(gold_data_sets)
        self.publish_data(gold_data_sets, spark)
        logging.info(
            'Created, validated & published gold datasets:'
            f' {[ds for ds in gold_data_sets.keys()]}'
        )


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
    **kwargs,
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


class SalesMartETL(StandardETL):
    def get_bronze_datasets(self, spark, **kwargs) -> Dict[str, DeltaDataSet]:
        customer_df, orders_df = generate_bronze_data(spark)
        return {
            'customer': DeltaDataSet(
                name='customer',
                curr_data=customer_df,
                primary_keys=['id'],
                storage_path=f'{self.STORAGE_PATH}/customer',
                table_name='customer',
                data_type='delta',
                schema=f'{self.SCHEMA}',
                partition=kwargs.get('partition'),
            ),
            'orders': DeltaDataSet(
                name='orders',
                curr_data=orders_df,
                primary_keys=['order_id'],
                storage_path=f'{self.STORAGE_PATH}/orders',
                table_name='orders',
                data_type='delta',
                schema=f'{self.SCHEMA}',
                partition=kwargs.get('partition'),
            ),
        }

    def get_dim_customer(
        self, customer: DeltaDataSet, spark, **kwargs
    ) -> DataFrame:
        customer_df = customer.curr_data
        dim_customer = spark.read.table(f'{self.SCHEMA}.dim_customer')
        # generete pk
        customer_df = customer_df.withColumn(
            'customer_sur_id',
            expr('md5(concat(id, datetime_updated))'),
        )
        # get only latest customer rows in dim_customer
        # since dim customer may have multiple rows per customer (SCD2)
        dim_customer_latest = dim_customer.where("current = true")

        # get net new rows to insert
        customer_df_insert_net_new = (
            customer_df.join(
                dim_customer_latest,
                (customer_df.id == dim_customer_latest.id)
                & (
                    dim_customer_latest.datetime_updated
                    < customer_df.datetime_updated
                ),
                'leftanti',
            )
            .select(
                customer_df.id,
                customer_df.customer_sur_id,
                customer_df.first_name,
                customer_df.last_name,
                customer_df.state_id,
                customer_df.datetime_created,
                customer_df.datetime_updated,
            )
            .withColumn('current', lit(True))
            .withColumn('valid_from', customer_df.datetime_updated)
            .withColumn('valid_to', lit('2099-01-01 12:00:00.0000'))
        )

        # get rows to insert for existing ids
        customer_df_insert_existing_ids = (
            customer_df.join(
                dim_customer_latest,
                (customer_df.id == dim_customer_latest.id)
                & (
                    dim_customer_latest.datetime_updated
                    < customer_df.datetime_updated
                ),
            )
            .select(
                customer_df.id,
                customer_df.customer_sur_id,
                customer_df.first_name,
                customer_df.last_name,
                customer_df.state_id,
                customer_df.datetime_created,
                customer_df.datetime_updated,
            )
            .withColumn('current', lit(True))
            .withColumn('valid_from', customer_df.datetime_updated)
            .withColumn('valid_to', lit('2099-01-01 12:00:00.0000'))
        )
        # get rows to be updated
        customer_df_ids_update = (
            dim_customer_latest.join(
                customer_df,
                (dim_customer_latest.id == customer_df.id)
                & (
                    dim_customer_latest.datetime_updated
                    < customer_df.datetime_updated
                ),
            )
            .select(
                dim_customer_latest.id,
                dim_customer_latest.customer_sur_id,
                dim_customer_latest.first_name,
                dim_customer_latest.last_name,
                dim_customer_latest.state_id,
                dim_customer_latest.datetime_created,
                customer_df.datetime_updated,
                dim_customer_latest.valid_from,
            )
            .withColumn('current', lit(False))
            .withColumn('valid_to', customer_df.datetime_updated)
        )
        return customer_df_insert_net_new.unionByName(
            customer_df_insert_existing_ids
        ).unionByName(customer_df_ids_update)

    def get_fct_orders(
        self, input_datasets: Dict[str, DeltaDataSet], spark, **kwargs
    ) -> DataFrame:
        dim_customer = input_datasets.get('dim_customer').curr_data
        orders_df = input_datasets.get('orders').curr_data

        dim_customer_curr_df = dim_customer.where("current = true")
        return orders_df.join(
            dim_customer_curr_df,
            orders_df.customer_id == dim_customer_curr_df.id,
            "left",
        ).select(
            orders_df.order_id,
            orders_df.customer_id,
            orders_df.item_id,
            orders_df.item_name,
            orders_df.delivered_on,
            orders_df.datetime_order_placed,
            dim_customer_curr_df.customer_sur_id,
        )

    def get_silver_datasets(
        self, input_datasets: Dict[str, DeltaDataSet], spark, **kwargs
    ) -> Dict[str, DeltaDataSet]:
        dim_customer_df = self.get_dim_customer(
            input_datasets.get('customer'), spark
        )

        silver_datasets = {}
        silver_datasets['dim_customer'] = DeltaDataSet(
            name='dim_customer',
            curr_data=dim_customer_df,
            primary_keys=['customer_sur_id'],
            storage_path=f'{self.STORAGE_PATH}/dim_customer',
            table_name='dim_customer',
            data_type='delta',
            schema=f'{self.SCHEMA}',
            partition=kwargs.get('partition'),
        )
        self.publish_data(silver_datasets, spark)
        silver_datasets['dim_customer'].curr_data = spark.read.table(
            f'{self.SCHEMA}.dim_customer'
        )
        silver_datasets['dim_customer'].skip_publish = True
        input_datasets['dim_customer'] = silver_datasets['dim_customer']

        silver_datasets['fct_orders'] = DeltaDataSet(
            name='fct_orders',
            curr_data=self.get_fct_orders(input_datasets, spark),
            primary_keys=['order_id'],
            storage_path=f'{self.STORAGE_PATH}/fct_orders',
            table_name='fct_orders',
            data_type='delta',
            schema=f'{self.SCHEMA}',
            partition=kwargs.get('partition'),
        )
        return silver_datasets

    def get_sales_mart(
        self, input_datasets: Dict[str, DeltaDataSet], **kwargs
    ) -> DataFrame:
        dim_customer = (
            input_datasets.get('dim_customer')
            .curr_data.where("current = true")
            .select("customer_sur_id", "state_id")
        )
        fct_orders = input_datasets.get('fct_orders').curr_data
        return (
            fct_orders.alias("fct_orders")
            .join(
                dim_customer.alias("dim_customer"),
                fct_orders.customer_sur_id == dim_customer.customer_sur_id,
                "left",
            )
            .select(
                expr('to_date(fct_orders.delivered_on, "yyyy-dd-mm")').alias(
                    "deliver_date"
                ),
                col("dim_customer.state_id").alias("state_id"),
            )
            .groupBy("deliver_date", "state_id")
            .count()
            .withColumnRenamed("count", "num_orders")
        )

    def get_gold_datasets(
        self, input_datasets: List[DeltaDataSet], spark, **kwargs
    ) -> Dict[str, DeltaDataSet]:
        sales_mart_df = self.get_sales_mart(input_datasets)
        return {
            'sales_mart': DeltaDataSet(
                name='sales_mart',
                curr_data=sales_mart_df,
                primary_keys=['state_id', 'deliver_date'],
                storage_path=f'{self.STORAGE_PATH}/sales_mart',
                table_name='sales_mart',
                data_type='delta',
                schema=f'{self.SCHEMA}',
                partition=kwargs.get('partition'),
            )
        }


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("adventureworks")
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    sm = SalesMartETL()
    partition = datetime.now().strftime(
        "%Y-%m-%d-%H"
    )  # usually from orchestrator
    sm.run(spark, partition=partition)
