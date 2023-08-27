from adventureworks.pipelines.sales_mart import SalesMartETL


class TestSalesMartETL:
    def test_get_bronze_datasets(self, spark) -> None:
        sm = SalesMartETL()
        bronze_datasets = sm.get_bronze_datasets(
            spark, partition="YYYY-MM-DD_HH"
        )
        assert len(bronze_datasets) == 2
        assert sorted(bronze_datasets.keys()) == ['customer', 'orders']
        assert bronze_datasets['customer'].curr_data.count() == 1000

    def test_get_dim_customer(self, spark) -> None:
        pass

    def test_get_fct_orders(self, spark) -> None:
        pass

    def test_get_sales_mart(self, spark) -> None:
        pass
