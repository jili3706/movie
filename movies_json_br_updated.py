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


