up:
	docker compose up --build -d

down:
	docker compose down

restart: down up

sh:
	docker exec -ti local-spark bash

meta:
	PGPASSWORD=sdepassword pgcli -h localhost -p 5432 -U sdeuser -d metadatadb

ddl-bronze:
	docker exec -ti local-spark bash -c '$$SPARK_HOME/bin/spark-submit --master local[*] --packages io.delta:$${DELTA_PACKAGE_VERSION},org.apache.hadoop:hadoop-aws:3.3.2 --conf spark.databricks.delta.retentionDurationCheck.enabled=false --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog --conf spark.hadoop.fs.s3a.access.key=minio --conf spark.hadoop.fs.s3a.secret.key=minio123 --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 --conf spark.hadoop.fs.s3a.region=us-east-1 --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem --conf spark.hadoop.fs.s3a.path.style.access=true ./adventureworks/ddl/create_bronze_tables.py'

ddl-silver:
	docker exec -ti local-spark bash -c '$$SPARK_HOME/bin/spark-submit --master local[*] --packages io.delta:$${DELTA_PACKAGE_VERSION},org.apache.hadoop:hadoop-aws:3.3.2 --conf spark.databricks.delta.retentionDurationCheck.enabled=false --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog --conf spark.hadoop.fs.s3a.access.key=minio --conf spark.hadoop.fs.s3a.secret.key=minio123 --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 --conf spark.hadoop.fs.s3a.region=us-east-1 --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem --conf spark.hadoop.fs.s3a.path.style.access=true ./adventureworks/ddl/create_silver_tables.py'

ddl-gold:
	docker exec -ti local-spark bash -c '$$SPARK_HOME/bin/spark-submit --master local[*] --packages io.delta:$${DELTA_PACKAGE_VERSION},org.apache.hadoop:hadoop-aws:3.3.2 --conf spark.databricks.delta.retentionDurationCheck.enabled=false --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog --conf spark.hadoop.fs.s3a.access.key=minio --conf spark.hadoop.fs.s3a.secret.key=minio123 --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 --conf spark.hadoop.fs.s3a.region=us-east-1 --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem --conf spark.hadoop.fs.s3a.path.style.access=true ./adventureworks/ddl/create_gold_tables.py'

ddl-interface:
	docker exec -ti local-spark bash -c '$$SPARK_HOME/bin/spark-submit --master local[*] --packages io.delta:$${DELTA_PACKAGE_VERSION},org.apache.hadoop:hadoop-aws:3.3.2 --conf spark.databricks.delta.retentionDurationCheck.enabled=false --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog --conf spark.hadoop.fs.s3a.access.key=minio --conf spark.hadoop.fs.s3a.secret.key=minio123 --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 --conf spark.hadoop.fs.s3a.region=us-east-1 --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem --conf spark.hadoop.fs.s3a.path.style.access=true ./adventureworks/ddl/create_interface_views.py'

ddl: ddl-bronze ddl-silver ddl-gold ddl-interface

etl:
	docker exec -ti local-spark bash -c '$$SPARK_HOME/bin/spark-submit --master local[*] --packages io.delta:$${DELTA_PACKAGE_VERSION},org.apache.hadoop:hadoop-aws:3.3.2 --conf spark.databricks.delta.retentionDurationCheck.enabled=false --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog --conf spark.hadoop.fs.s3a.access.key=minio --conf spark.hadoop.fs.s3a.secret.key=minio123 --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 --conf spark.hadoop.fs.s3a.region=us-east-1 --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem --conf spark.hadoop.fs.s3a.path.style.access=true ./adventureworks/pipelines/sales_mart.py'

spark-sh:
	docker exec -ti local-spark bash -c '$$SPARK_HOME/bin/spark-shell --packages io.delta:$${DELTA_PACKAGE_VERSION},org.apache.hadoop:hadoop-aws:3.3.2 --conf spark.hadoop.fs.s3a.access.key=minio --conf spark.hadoop.fs.s3a.secret.key=minio123 --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 --conf spark.hadoop.fs.s3a.region=us-east-1 --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem --conf spark.hadoop.fs.s3a.path.style.access=true --conf spark.databricks.delta.retentionDurationCheck.enabled=false --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog'


######################################################################################################

pytest:
	docker exec -ti local-spark bash -c 'python3 -m pytest --log-cli-level info -p no:warnings -v ./adventureworks/tests'

format:
	docker exec -ti local-spark bash -c 'python3 -m black -S --line-length 79 --preview ./adventureworks'
	docker exec -ti local-spark bash -c 'isort ./adventureworks'

type:
	docker exec -ti local-spark bash -c 'python3 -m mypy --no-implicit-reexport --ignore-missing-imports --no-namespace-packages ./adventureworks'

lint:
	docker exec -ti local-spark bash -c 'flake8 ./adventureworks'
	docker exec -ti local-spark bash -c 'flake8 ./adventureworks/tests'

ci: format type lint pytest