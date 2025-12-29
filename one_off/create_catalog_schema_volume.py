# Databricks notebook source
# MAGIC %sql
# MAGIC create catalog if not exists nyctaxi managed location 'abfss://unity-catalog-storage@dbstoragec3j3wabfvp4ro.dfs.core.windows.net/7405606178373796';

# COMMAND ----------

spark.sql("create schema if not exists nyctaxi.00_landing")
spark.sql("create schema if not exists nyctaxi.01_bronze")
spark.sql("create schema if not exists nyctaxi.02_silver")
spark.sql("create schema if not exists nyctaxi.03_gold")

# COMMAND ----------

spark.sql("create volume if not exists nyctaxi.00_landing.data_sources")