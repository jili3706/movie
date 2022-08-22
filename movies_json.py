# Databricks notebook source
from pyspark.sql import DataFrame

df_for_josns=DataFrameReader.json(/Users/jinyu/Downloads/movie_final_project)

DELTALAKE_BRONZE_PATH = "dbfs:/FileStore/Bronze_Movies "

df_for_josns.write.format('delta').mode('overwrite').save(DELTALAKE_BRONZE_PATH)

spark.sql(f"CREATE TABLE bronze_movies USING delta LOCATION '{DELTALAKE_BRONZE_PATH}'") 

movies_tb = spark.read.format("delta").load(DELTALAKE_BRONZE_PATH)

display(movies_tb)

# COMMAND ----------


