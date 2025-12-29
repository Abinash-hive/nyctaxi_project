# Databricks notebook source
from pyspark.sql.functions import current_timestamp,lit,col
from pyspark.sql.types import IntegerType,timestampType


# COMMAND ----------

df = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/Volumes/nyctaxi/00_landing/data_sources/lookup/*")

# COMMAND ----------

df.selectExpr(
    "LocationID as location_id",
    "Borough as borough",
    "Zone as zone",
    "service_zone as service_zone",
    "current_timestamp() as effective_date",
    "cast(null as timestamp) as end_date"
  ).write.mode("overwrite").saveAsTable("nyctaxi.02_silver.taxi_zone_lookup")
