# SCD Type 2 Implementation with Delta Lake

It updates existing customer records with historical tracking (using effective_start_date, effective_end_date, and active flags) and inserts new customer records based on sales data.
The project simulates a real-world customer master dataset, incoming sales transactions, and uses a Delta Merge strategy to perform SCD Type 2 tracking.

# Tech Stack
Databricks - Cloud-based Spark platform
Delta Lake - ACID transactions, time travel
PySpark	- Data processing logic
Python - Glue logic
DBFS	- Databricks File System storage

# Features
Complete SCD Type 2 implementation with merge operations

Address change detection logic

Data quality checks pre-merge

Production-ready patterns:

Window functions for latest records

Deduplication handling

Delta Lake optimizations
