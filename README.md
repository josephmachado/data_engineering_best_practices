# Data Engineering Best Practices

WIP

# Setup 

# Run 

Add this to 

```bash
make up
make ddl
make etl
```

Validate that your code ran, open spark shell with `make spark-sh`

```scala
spark.sql("select partition from adventureworks.sales_mart group by 1").show() // should be the number of times you ran `make etl`
spark.sql("select * from businessintelligence.sales_mart").show(100) // should be the latest partition 59 rows
spark.sql("select count(*) from businessintelligence.sales_mart").show() // 59
spark.sql("select count(*) from adventureworks.dim_customer").show() // 1000 X  num of etl runs
spark.sql("select count(*) from adventureworks.fct_orders").show() // 10000 X  num of etl runs

spark.sql("select * from adventureworks.sales_mart limit 5").show()
spark.sql("select * from adventureworks.sales_mart limit 5").show()
spark.sql("select * from adventureworks.dim_customer where current = false").show()
spark.sql("select * from adventureworks.fct_orders limit 3").show()
```

## Check validation store

```bash
make meta
```

```sql
select * from ge_validations_store  limit 1;
exit
```

# Code design

