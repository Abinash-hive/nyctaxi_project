# Databricks notebook source
# MAGIC %sql
# MAGIC select vendor, sum(total_amount) from nyctaxi.02_silver.yellow_trips_enriched group by vendor order by 2 desc limit 1

# COMMAND ----------

df = spark.read.table("nyctaxi.02_silver.yellow_trips_enriched")

# COMMAND ----------

from pyspark.sql.functions import *

display(
    df.groupBy("vendor")
      .agg(round(sum("total_amount"),1).alias("total_revenue"))
      .orderBy("total_revenue", ascending=False)
      .limit(1)
)

# COMMAND ----------

display(
    df.groupBy("pu_borough")
      .agg(round(count("*"),1).alias("number_of_trips"))
      .orderBy("number_of_trips", ascending=False)
      .limit(1)
)

# COMMAND ----------

display(
    df.groupBy(concat("pu_borough",lit('-->'),"do_borough"))
      .agg(round(count("*"),1).alias("number_of_trips"))
      .orderBy("number_of_trips", ascending=False)
      
)

# COMMAND ----------

df=spark.read.table("nyctaxi.03_gold.daily_trip_summary")

# COMMAND ----------

display(df)