# Databricks notebook source
sales_dataset = [
    (1, 1, 'Virat', '2023-01-16', 'gurgaon', 'india', 380),
    (77, 1, 'Virat', '2023-03-11', 'bangalore', 'india', 300),
    (12, 3, 'Gill', '2023-09-20', 'Amritsar', 'india', 127),
    (54, 4, 'Rahul', '2023-08-10', 'jaipur', 'india', 321),
    (65, 5, 'Rinku', '2023-09-07', 'mosco', 'russia', 765),
    (89, 6, 'SKY', '2023-08-10', 'jaipur', 'india', 321)
]
sales_schema = ['sales_id', 'customer_id', 'customer_name', 'sales_date', 'food_delivery_address', 'food_delivery_country', 'food_cost']

sales_df = spark.createDataFrame(sales_dataset, sales_schema)

sales_df = sales_df.withColumn('sales_date', to_date('sales_date', 'yyyy-MM-dd'))