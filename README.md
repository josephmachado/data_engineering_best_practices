# Data Engineering Best Practices

WIP

# Setup 

# Run 

```bash
make up
make ddl
make etl
```

Validate that your code ran, open spark shell with `make spark-sh`

```scala
spark.sql("select * from adventureworks.sales_mart").show()
```

# Code design

