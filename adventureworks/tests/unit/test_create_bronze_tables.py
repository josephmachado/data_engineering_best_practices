from adventureworks.ddl.create_bronze_tables import create_tables, drop_tables


def test_create_tables(spark):
    create_tables(spark, path="/tmp-test/delta")
    dim_customer_df = spark.sql("select * from adventureworks.dim_customer")
    assert sorted(dim_customer_df.columns) == [
        'current',
        'customer_sur_id',
        'datetime_created',
        'datetime_updated',
        'first_name',
        'id',
        'last_name',
        'state_id',
        'valid_from',
        'valid_to',
    ]
    drop_tables(spark)
