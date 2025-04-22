#  Project Title
> SCD Type 2 Implementation with Delta Lake

## Project Description
It updates existing customer records with historical tracking (using effective_start_date, effective_end_date, and active flags) and inserts new customer records based on sales data.
The project simulates a real-world customer master dataset, incoming sales transactions, and uses a Delta Merge strategy to perform SCD Type 2 tracking.

## Tech Stack
- Databricks - Cloud-based Spark platform
- Delta Lake - ACID transactions, time travel
- PySpark	- Data processing logic
- Python - Glue logic
- DBFS	- Databricks File System storage

# Features
- Complete SCD Type 2 implementation with merge operations
- Address change detection logic
- Data quality checks pre-merge
- Production-ready patterns:
- Window functions for latest records
- Deduplication handling
- Delta Lake optimizations

# Project Structure
```bash

SCD-Type-2-Implementation-with-Delta-Lake/
├── 01_create_customer_data.py             # Script to create and save initial customer dataset as Delta table
├── 02_create_sales_data.py                # Script to create and prepare sales dataset
├── 03_merge_logic_scd2.py                 # Script that performs SCD2 merge logic between sales and customer data
├── README.md                              # Full project explanation

```

## PySpark/Databricks Topics covered
- PySpark DataFrame operations (filter, join, withColumn, concat, dropDuplicates, etc.)  [https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html](url)
- Window functions (row_number, partitionBy, orderBy) [https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/968100988546031/157591980591166/8836542754149149/latest.html](url)
- Delta Lake MERGE INTO (UPSERT) operation [https://docs.databricks.com/aws/en/delta/merge](url)
- SCD Type 2 handling (marking old records inactive, inserting new records)
- Databricks DBFS file storage and management [https://docs.databricks.com/aws/en/files/](url)

## Important Functions/Commands used
```DeltaTable.forPath()```,
```merge()```, ```whenMatchedUpdate()```, ```whenNotMatchedInsert()```,
```Window.partitionBy().orderBy()```,
```row_number().over()```,
```concat()```, ```col()```, ```lit()```
