# Databricks notebook source
customer_dataset = [
    (1, 'Virat', 'Kolkata', 'India', 'N', '2021-09-15', '2022-09-24'),
    (2, 'Rohit', 'Mumbai', 'India', 'Y', '2022-08-12', None),
    (3, 'Gill', 'Amritsar', 'India', 'Y', '2022-09-10', None),
    (4, 'Rahul', 'Delhi', 'India', 'Y', '2022-06-10', None),
    (5, 'Rinku', 'NY', 'USA', 'Y', '2022-06-10', None),
    (1, 'Virat', 'Gurgaon', 'India', 'Y', '2022-09-25', None)
]
customer_schema = ['id', 'name', 'city', 'country', 'active', 'effective_start_date', 'effective_end_date']

customer_df = spark.createDataFrame(customer_dataset, customer_schema)

customer_df = customer_df.withColumn('effective_start_date', to_date('effective_start_date', 'yyyy-MM-dd'))
customer_df = customer_df.withColumn('effective_end_date', to_date('effective_end_date', 'yyyy-MM-dd'))

customer_df.show(truncate=False)

customer_df.write.format("delta").save("dbfs:/FileStore/SCDtype2customerDataset")