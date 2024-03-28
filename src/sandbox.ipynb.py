# Databricks notebook source
import csv
out = f"../output_data/uk_ids.csv"
with open(out, mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(["azure_path", "company_id", "products_and_services"])
    file.close()

# COMMAND ----------

uk_files = [x[0] for x in dbutils.fs.ls("abfss://landingzone@storagetiltdevelop.dfs.core.windows.net/tiltEP/") if "/united-kingdom" in x[0]]

# COMMAND ----------

import pandas as pd

# COMMAND ----------

for file in uk_files:
    x = spark.read.csv(file,header=True, inferSchema=True, sep=";").toPandas()
    for id in list(x.drop_duplicates(subset="id")["id"]):
        with open(out, 'a') as file:
            writer = csv.writer(file)
            writer.writerow([file, id,""])
            file.close()

