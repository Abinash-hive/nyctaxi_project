# Databricks notebook source
import urllib.request as re
import shutil
import os

# COMMAND ----------

url = 'https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv'

os.makedirs('/Volumes/nyctaxi/00_landing/data_sources/nyctaxi_yellow/lookup', exist_ok=True)

local_path = '/Volumes/nyctaxi/00_landing/data_sources/nyctaxi_yellow/lookup/taxi_zone_lookup.csv'
with re.urlopen(url) as response:
    with open(local_path, 'wb') as f: shutil.copyfileobj(response, f)

# COMMAND ----------

dbutils.fs.mv("/Volumes/nyctaxi/00_landing/data_sources/nyctaxi_yellow/lookup", "/Volumes/nyctaxi/00_landing/data_sources/lookup", True)