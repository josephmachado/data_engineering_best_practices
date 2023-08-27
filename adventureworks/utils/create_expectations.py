##########################################################################################
## NOT TO BE RUN AS A SCRIPT !!!
##########################################################################################

import os

# Read expectation context
import great_expectations as gx
from great_expectations.core.expectation_configuration import \
    ExpectationConfiguration

context = gx.get_context(
    context_root_dir=os.path.join(
        os.getcwd(), "adventureworks", "great_expectations"
    )
)

datasource = context.sources.add_spark("spark_datasource")
context.add_or_update_checkpoint(name="dq_checkpoint")


def create_bronze_expectations(context, datasource):
    # Create expectation for orders and customer
    context.add_or_update_expectation_suite(expectation_suite_name="orders")
    orders_suite = context.get_expectation_suite(
        expectation_suite_name="orders"
    )

    # Create an Expectation for order_id to be unique
    order_id_column_unique = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_unique",
        kwargs={"column": "order_id"},
        meta={
            "notes": {
                "format": "markdown",
                "content": "order_id column is expected to be unique",
            }
        },
    )
    # Add the Expectation to the suite
    orders_suite.add_expectation(
        expectation_configuration=order_id_column_unique
    )
    # Save the expectation to file system
    context.save_expectation_suite(expectation_suite=orders_suite)

    context.add_or_update_expectation_suite(expectation_suite_name="customer")
    customer_suite = context.get_expectation_suite(
        expectation_suite_name="customer"
    )
    # Create an Expectation for id to be not nul
    column_id_not_null = ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs={"column": "id"},
        meta={
            "notes": {
                "format": "markdown",
                "content": "id column is expected to be not null",
            }
        },
    )
    # Add the Expectation to the suite
    customer_suite.add_expectation(
        expectation_configuration=column_id_not_null
    )
    # Save the expectation to file system
    context.save_expectation_suite(expectation_suite=customer_suite)

    # Create datasource and assets

    datasource.add_dataframe_asset(name="orders")
    datasource.add_dataframe_asset(name="customer")


def create_silver_expectations(context, datasource):
    # Create expectation for orders and customer
    context.add_or_update_expectation_suite(
        expectation_suite_name="fct_orders"
    )
    fct_orders_suite = context.get_expectation_suite(
        expectation_suite_name="fct_orders"
    )

    # Create an Expectation for order_id to be unique
    order_id_column_unique = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_unique",
        kwargs={"column": "order_id"},
        meta={
            "notes": {
                "format": "markdown",
                "content": "order_id column is expected to be unique",
            }
        },
    )
    # Add the Expectation to the suite
    fct_orders_suite.add_expectation(
        expectation_configuration=order_id_column_unique
    )
    # Save the expectation to file system
    context.save_expectation_suite(expectation_suite=fct_orders_suite)

    context.add_or_update_expectation_suite(
        expectation_suite_name="dim_customer"
    )
    dim_customer_suite = context.get_expectation_suite(
        expectation_suite_name="dim_customer"
    )
    # Create an Expectation for id to be not nul
    column_id_not_null = ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs={"column": "id"},
        meta={
            "notes": {
                "format": "markdown",
                "content": "id column is expected to be not null",
            }
        },
    )
    # Add the Expectation to the suite
    dim_customer_suite.add_expectation(
        expectation_configuration=column_id_not_null
    )
    # Save the expectation to file system
    context.save_expectation_suite(expectation_suite=dim_customer_suite)

    datasource.add_dataframe_asset(name="fct_orders")
    datasource.add_dataframe_asset(name="dim_customer")


# test run checkpoint
# run in pyspark shell
from adventureworks.pipelines.sales_mart import generate_bronze_data

customer_df, orders_df = generate_bronze_data(spark)

results = context.run_checkpoint(
    checkpoint_name="dq_checkpoint",
    validations=[
        {
            "batch_request": context.get_datasource("spark_datasource")
            .get_asset("orders")
            .build_batch_request(dataframe=orders_df),
            "expectation_suite_name": "orders",
        },
        {
            "batch_request": context.get_datasource("spark_datasource")
            .get_asset("customer")
            .build_batch_request(dataframe=customer_df),
            "expectation_suite_name": "customer",
        },
    ],
)

results_dict = results.list_validation_results()
# results_dict[0].get('success')
