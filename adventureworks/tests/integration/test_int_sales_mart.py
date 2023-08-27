from adventureworks.ddl.create_silver_tables import \
    create_tables as create_silver_tables
from adventureworks.ddl.create_silver_tables import \
    drop_tables as drop_silver_tables
from adventureworks.pipelines.sales_mart import SalesMartETL


class TestSalesMartETL:
    def test_get_validate_publish_silver_datasets(self, spark) -> None:
        sm = SalesMartETL("/tmp/delta", database="adventureworks_test")
        bronze_datasets = sm.get_bronze_datasets(
            spark, partition="YYYY-MM-DD_HH"
        )
        create_silver_tables(
            spark, path="/tmp/delta", database="adventureworks_test"
        )
        silver_datasets = sm.get_silver_datasets(bronze_datasets, spark)
        drop_silver_tables(spark, database="adventureworks_test")
        assert sorted(silver_datasets.keys()) == ['dim_customer', 'fct_orders']

    def test_get_validate_publish_bronze_datasets(self, spark) -> None:
        pass

    def test_get_validate_publish_gold_datasets(self, spark) -> None:
        pass
