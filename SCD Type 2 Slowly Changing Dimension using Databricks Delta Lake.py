# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import concat
from pyspark.sql.functions import lit
from delta.tables import DeltaTable



spark = SparkSession.builder.getOrCreate()

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



# COMMAND ----------

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



# COMMAND ----------

deltatable = DeltaTable.forPath(spark, "dbfs:/FileStore/SCDtype2customerDataset")
cust_df = deltatable.toDF()
cust_df.show()
sales_df.show()




# COMMAND ----------

join_df = sales_df.join(cust_df, (cust_df.id==sales_df.customer_id),'left')
join_df.display()



# COMMAND ----------

filter_df = join_df.filter((join_df.id.isNull()) | ((join_df.active=='Y') & (join_df.food_delivery_address!=join_df.city))).withColumn('comp_key',concat(join_df.customer_id,join_df.name))
filter_df.display()



# COMMAND ----------

change_df = filter_df.filter(filter_df.id.isNotNull()).withColumn('comp_key', lit(None))
change_df.display()


# COMMAND ----------

merge_df=filter_df.union(change_df)
merge_df.display()



# COMMAND ----------

merge_df.groupBy("comp_key").count().filter("count > 1").show()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from pyspark.sql.functions import col


# For non-null comp_keys (the updates)
window_spec = Window.partitionBy("comp_key").orderBy(col("sales_date").desc())
updates_df = merge_df.filter("comp_key is not null") \
                   .withColumn("row_num", row_number().over(window_spec)) \
                   .filter("row_num = 1") \
                   .drop("row_num")

# For null comp_keys (the inserts) - take distinct rows
inserts_df = merge_df.filter("comp_key is null").dropDuplicates()

# Recombine the cleaned DataFrames
clean_merge_df = updates_df.union(inserts_df)

# Now perform the merge with clean data
deltatable.alias("target").merge(
    clean_merge_df.alias("source"),
    "concat(target.id, target.name) = source.comp_key and target.active = 'Y'"
).whenMatchedUpdate(
    set={
        "active": lit("N"),
        "effective_end_date": col("source.sales_date")
    }
).whenNotMatchedInsert(
    values={
        "id": "source.customer_id",
        "name": "source.customer_name",
        "city": "source.food_delivery_address",
        "country": "source.food_delivery_country",
        "active": lit("Y"),
        "effective_start_date": "source.sales_date",
        "effective_end_date": lit(None).cast("date")
    }
).execute()

# COMMAND ----------

df=spark.read.format('delta').load('dbfs:/FileStore/SCDtype2customerDataset')
df.display()

# COMMAND ----------

