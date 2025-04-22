# Databricks notebook source
deltatable = DeltaTable.forPath(spark, "dbfs:/FileStore/SCDtype2customerDataset")

cust_df = deltatable.toDF()

cust_df.show()
sales_df.show()

join_df = sales_df.join(cust_df, (cust_df.id==sales_df.customer_id),'left')
join_df.display()

filter_df = join_df.filter((join_df.id.isNull()) | ((join_df.active=='Y') & (join_df.food_delivery_address!=join_df.city))).withColumn('comp_key',concat(join_df.customer_id,join_df.name))
filter_df.display()

change_df = filter_df.filter(filter_df.id.isNotNull()).withColumn('comp_key', lit(None))
change_df.display()

merge_df=filter_df.union(change_df)
merge_df.display()

merge_df.groupBy("comp_key").count().filter("count > 1").show()

window_spec = Window.partitionBy("comp_key").orderBy(col("sales_date").desc())
updates_df = merge_df.filter("comp_key is not null") \
                   .withColumn("row_num", row_number().over(window_spec)) \
                   .filter("row_num = 1") \
                   .drop("row_num")

inserts_df = merge_df.filter("comp_key is null").dropDuplicates()

clean_merge_df = updates_df.union(inserts_df)

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

df=spark.read.format('delta').load('dbfs:/FileStore/SCDtype2customerDataset')
df.display()