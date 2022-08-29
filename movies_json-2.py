# Databricks notebook source
from pyspark.sql.functions import current_timestamp, lit

from pyspark.sql.functions import explode, col

from pyspark.sql.functions import abs
classicPath = f"/FileStore/tables/classic/"
landingPath = classicPath + "landing/"

bronzePath = classicPath + "bronze/"
silverPath = classicPath + "silver/"


df  = spark.read.option("multiline", "true").json("/FileStore/tables")

movies = df.select(explode("movie").alias("movies"))



kafka_schema = "movies Struct"
bronze_df = movies.select(
    "movies",
    lit("movieshop_final_project.file").alias("datasource"),
    current_timestamp().alias("ingesttime"),
    lit("needs to be updated").alias("status"),
    current_timestamp().cast("date").alias("ingestdate"),
)

bronze_df.write.mode("overwrite").format("delta").saveAsTable("movie_df_delta_bronze")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from movie_df_delta_bronze

# COMMAND ----------


raw_df_ = movies.select(
    col("movies.Id").alias("ID"),
   col("movies.BackdropUrl").alias("BUrl")
 , col("movies.Budget").alias("Budget")
 , col("movies.CreatedBy").alias("Creators")
 , col("movies.CreatedDate").alias("CreatedDate")
    , col("movies.ImdbUrl").alias("ImdbUrl")
    , col("movies.OriginalLanguage").alias("Language")
    , col("movies.Overview").alias("Overviews")
    , col("movies.PosterUrl").alias("PosterUrl")
    , col("movies.Price").alias("Price")
    , col("movies.ReleaseDate").alias("ReleaseDate")
    , col("movies.Revenue").alias("Revenue")
    , col("movies.Runtime").alias("Runtime")
    , col("movies.Tagline").alias("Tagline")
     , col("movies.Title").alias("Title")
     , col("movies.TmdbUrl").alias("TmdbUrl")
     , col("movies.UpdatedBy").alias("UpdatedBy")
     , col("movies.UpdatedDate").alias("UpdatedDate")
 , explode("movies.genres").alias("Genres")
)

raw_df = raw_df_.select(
     "ID","BUrl","Budget","Creators","CreatedDate","ImdbUrl","Language","Overviews","PosterUrl","Price","ReleaseDate","Revenue","Runtime","Tagline","Title","TmdbUrl",
    "UpdatedBy","UpdatedDate",
    col("Genres.id").alias("Genres_id"),
    col("Genres.name").alias("Genres_name") )


clean_df = raw_df.withColumn('Runtime', abs(raw_df.Runtime))
silver_df = clean_df.select(
    "ID","BUrl","Budget","Creators","CreatedDate","ImdbUrl","Language","Overviews","PosterUrl","Price","ReleaseDate","Revenue","Runtime","Tagline","Title","TmdbUrl",
    "UpdatedBy","UpdatedDate","Genres_id","Genres_name",
    lit("movieshop_final_project.file").alias("datasource"),
    current_timestamp().alias("ingesttime"),
    lit("raw").alias("status"),
    current_timestamp().cast("date").alias("ingestdate"),
   
).write.mode("overwrite").format("delta").saveAsTable("movie_df_delta_silver")

# COMMAND ----------

Genres_movie_lookup = clean_df.select("ID", "Genres_id", "Genres_name").dropDuplicates()
display(Genres_movie_lookup)
Genres_movie_lookup_transit = Genres_movie_lookup.select(
    "ID",
    "Genres_id",
    "Genres_name"
)
display(Genres_movie_lookup_transit)

Language_movie_lookup= clean_df.select("Language").dropDuplicates()
Language_movie_lookup_transit = Language_movie_lookup.select(
    lit("Language_id"),
    "Language"
)
Language_movie_lookup_silver = Language_movie_lookup_transit.withColumn("Language_id", lit("1")).withColumn("Language",lit("en"))
display(Language_movie_lookup_silver)

Movie_Silver_Tb = clean_df.withColumn(
    "Language_id", lit("1")
)
display(Movie_Silver_Tb)


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Language_movie_lookup_silver_delta_table

# COMMAND ----------



# COMMAND ----------


