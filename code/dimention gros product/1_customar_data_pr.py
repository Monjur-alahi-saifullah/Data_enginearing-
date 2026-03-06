# Databricks notebook source
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %run /Workspace/pipeline/setup_1/utilities

# COMMAND ----------

print(bronze_s,silver_s,gold_s)

# COMMAND ----------

dbutils.widgets.text("catalog","fmgc","catalog")

# COMMAND ----------

dbutils.widgets.text("data_source","customer","data_source")

# COMMAND ----------

dbutils.widgets.get("data_source")

# COMMAND ----------

import pandas as pd

# Use quotes around the path
pdf = pd.read_csv("/Workspace/pipeline/full_data/customars/customers.csv")
pdf.head()

# COMMAND ----------

dbutils.widgets.get("catalog")

# COMMAND ----------

dbutils.widgets.text("catalog", "fmcg", "Catalog")
dbutils.widgets.text("data_source", "customers", "Data Source")

catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")

# COMMAND ----------

df_bronze = spark.sql(f"SELECT * FROM {catalog}.{bronze_s}.{data_source};")
df_bronze.show(10)

# COMMAND ----------

df_bronze.printSchema()

# COMMAND ----------

 df_dup=df_bronze.groupBy("customer_id").count().filter(F.col("count")>1)
 display(df_dup)

# COMMAND ----------

print('Rows before duplicates dropped: ', df_bronze.count())
df_silver = df_bronze.dropDuplicates(['customer_id'])
print('Rows after duplicates dropped: ', df_silver.count())

# COMMAND ----------

display(
    df_silver.filter(F.col("customer_name") != F.trim(F.col("customer_name")))
)

# COMMAND ----------

## remove those trim values

df_silver = df_silver.withColumn(
    "customer_name",
    F.trim(F.col("customer_name"))
)

# COMMAND ----------

display(
    df_silver.filter(F.col("customer_name") != F.trim(F.col("customer_name")))
)
# hete the out pur indicate theat the trimd rows been removed 

# COMMAND ----------

df_silver.select("city").distinct().show()

# COMMAND ----------

city_mapping={
    'Bengaluruu': 'Bengaluru',
    'Bengalore': 'Bengaluru',

    'Hyderabadd': 'Hyderabad',
    'Hyderbad': 'Hyderabad',

    'NewDelhi': 'New Delhi',
    'NewDheli': 'New Delhi',
    'NewDelhee': 'New Delhi'
}

df_silver=(df_silver.replace(city_mapping,subset=["city"]))

# COMMAND ----------

allowed = ["Bengaluru", "Hyderabad", "New Delhi"]
df_silver=df_silver.withColumn(
"city",
 F.when(F.col("city").isNull(),None)
  .when(F.col("city").isin(allowed),F.col("city"))
  .otherwise(None))
display(df_silver)
df_silver

# COMMAND ----------

df_silver.select('customer_name').distinct().show()

# COMMAND ----------

df_silver=df_silver.withColumn(
    "customer_name",
    F.when(F.col("customer_name").isNull(),None)
    .otherwise(F.initcap("customer_name"))
)

# COMMAND ----------

df_silver.select("customer_name").distinct().show(truncate=False)

# COMMAND ----------

df_silver.filter(F.col("city").isNull()).show(truncate=False)


# COMMAND ----------

null_customer_names = ['Sprintx Nutrition', 'Zenathlete Foods', 'Primefuel Nutrition', 'Recovery Lane']
df_silver.filter(F.col("customer_name").isin(null_customer_names)).show(truncate=False)

# COMMAND ----------


# Business Confirmation Note: City corrections confirmed by business team
customer_city_fix = {
    # Sprintx Nutrition
    789403: "New Delhi",

    # Zenathlete Foods
    789420: "Bengaluru",

    # Primefuel Nutrition
    789521: "Hyderabad",

    # Recovery Lane
    789603: "Hyderabad"
}

df_fix = spark.createDataFrame(
    [(k, v) for k, v in customer_city_fix.items()],
    ["customer_id", "fixed_city"]
)

display(df_fix)

# COMMAND ----------

df_silver = (
    df_silver
    .join(df_fix, "customer_id", "left")
    .withColumn(
        "city",
        F.coalesce("city", "fixed_city")   # Replace null with fixed city
    )
    .drop("fixed_city")
)

# COMMAND ----------

# Sanity Checks

null_customer_names = ['Sprintx Nutrition', 'Zenathlete Foods', 'Primefuel Nutrition', 'Recovery Lane']
df_silver.filter(F.col("customer_name").isin(null_customer_names)).show(truncate=False)

# COMMAND ----------

df_silver=df_silver.withColumn("customer_id",F.col("customer_id").cast("string"))
print(df_silver.printSchema())

# COMMAND ----------

df_silver = (
    df_silver
    # Build final customer column: "CustomerName-City" or "CustomerName-Unknown"
    .withColumn(
        "customer",
        F.concat_ws("-", "customer_name", F.coalesce(F.col("city"), F.lit("Unknown")))
    )
    
    # Static attributes aligned with parent data model
    .withColumn("market", F.lit("India"))
    .withColumn("platform", F.lit("Sports Bar"))
    .withColumn("channel", F.lit("Acquisition"))
)

# COMMAND ----------

display(df_silver.limit(5))

# COMMAND ----------

df_silver.write\
 .format("delta") \
 .option("delta.enableChangeDataFeed", "true") \
 .option("mergeSchema", "true") \
 .mode("overwrite") \
 .saveAsTable(f"{catalog}.{silver_s}.{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC gold stage 

# COMMAND ----------

df_silver = spark.sql(f"SELECT * FROM {catalog}.{silver_s}.{data_source};")


# take req cols only
# "customer_id, customer_name, city, read_timestamp, file_name, file_size, customer, market, platform, channel"
df_gold = df_silver.select("customer_id", "customer_name", "city", "customer", "market", "platform", "channel")

# COMMAND ----------

df_gold.write\
 .format("delta") \
 .option("delta.enableChangeDataFeed", "true") \
 .mode("overwrite") \
 .saveAsTable(f"{catalog}.{gold_s}.sb_dim_{data_source}")

# COMMAND ----------

delta_table = DeltaTable.forName(spark, "fmcg.gold.dim_customers")
df_child_customers = spark.table("fmcg.gold.sb_dim_customers").select(
    F.col("customer_id").alias("customer_code"),
    "customer",
    "market",
    "platform",
    "channel"
)

# COMMAND ----------

delta_table.alias("target").merge(
    source=df_child_customers.alias("source"),
    condition="target.customer_code = source.customer_code"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

