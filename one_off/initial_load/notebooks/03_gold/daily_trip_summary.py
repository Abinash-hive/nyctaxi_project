# Databricks notebook source
from pyspark.sql.functions import count, max, min, avg, sum, round

# COMMAND ----------

df = spark.read.table("nyctaxi.02_silver.yellow_trips_enriched")

# COMMAND ----------

df = df.groupBy(df.tpep_pickup_datetime.cast("date").alias("pickup_date")).\
agg(
count("*").alias("total_trips"),
round(avg('passenger_count'),1).alias("avg_passenger_per_trip"),
round(avg('trip_distance'),1).alias("avg_distance_per_trip"),
round(avg('fare_amount'),2).alias("avg_fare_per_trip"),
max('fare_amount').alias('max_fare'),
min('fare_amount').alias('min_fare'),
round(sum('total_amount'),1).alias('total_revenue')
)

# COMMAND ----------

df.write.mode("overwrite").saveAsTable("nyctaxi.03_gold.daily_trip_summary")