# Databricks notebook source
# MAGIC %md ###### Import libraries and dataset

# COMMAND ----------

from pyspark.sql.functions import split, when, size, col, to_date, unix_timestamp, from_unixtime, explode, initcap, expr, udf, StringType, trim, regexp_replace, col, array, lower
import re
from pyspark.sql.types import ArrayType, StringType

file_path = "/mnt/demo/RBL/raw/NetflixOriginals.csv"
df = spark.read.option("header", "true").csv(file_path)
display(df)

# COMMAND ----------

# MAGIC %md ######Format Premiere column to correct date format

# COMMAND ----------

df_2 = df.withColumn("Premiere", to_date(unix_timestamp(df["Premiere"], "MMMM d, yyyy").cast("timestamp")))
display(df_2)

# COMMAND ----------

# MAGIC %md ######Split Languages column into separate rows if show is in multiple langauges

# COMMAND ----------

df_3 = df_2.withColumn("Language", explode(split(col("Language"), "/")))
display(df_3)

# COMMAND ----------

# MAGIC %md ###### List all unique genres, matched by Title in the dataset

# COMMAND ----------

df_4 = df_3.select("Genre", "Title").distinct().orderBy("Genre")
display(df_4)

# COMMAND ----------

# MAGIC %md ######Split Genres with "/" into separate rows

# COMMAND ----------

df_5 = df_4.withColumn("Genre", explode(split(col("Genre"), "/"))).distinct().orderBy("Genre")
display(df_5)

# COMMAND ----------

# MAGIC %md ######Remove leading and trailing Whitespace on the Genre column

# COMMAND ----------

df_6 = df_5.withColumn("Genre", trim(col("Genre"))).distinct().orderBy("Genre")
display(df_6)

# COMMAND ----------

# MAGIC %md ######Fix Spelling mistake "Musicial" to "Musical"

# COMMAND ----------

df_7 = df_6.withColumn("Genre", when(col("Genre") == "Musicial", "Musical").otherwise(col("Genre"))).distinct()
display(df_7)

# COMMAND ----------

# MAGIC %md ######Make the first letter of each word uppercase

# COMMAND ----------

capitalize_words_udf = udf(lambda genre: '-'.join(word.capitalize() for word in re.split(r'\s+|-', genre)), StringType())

df_8 = df_7.withColumn("Genre", capitalize_words_udf(col("Genre"))).orderBy("Genre")
display(df_8)

# COMMAND ----------

# MAGIC %md ######Genre Standardisation 1: Remove "-Film", "-Show", "Coming-Of-Age-" & "Hidden-Camera-Prank-" strings from the Genre list

# COMMAND ----------

def process_genre(value):
    # Remove specified substrings
    value = value.replace("-Film", "").replace("-Show", "").replace("Coming-Of-Age-", "").replace("Hidden-Camera-Prank-", "")
    
    # Rename specific values
    value = value.replace("Romantic", "Romance").replace("Animated", "Animation").replace("Teenage", "Teen")

    # Capitalize logic
    if "One-Man" in value:
        return "One-man show"
    else:
        return '-'.join(word.capitalize() for word in value.split('-'))

# Register the UDF
process_genre_udf = udf(process_genre, StringType())

# Apply the UDF to the DataFrame
df_9 = df_8.withColumn("Genre", process_genre_udf(col("Genre")))
display(df_9)

# COMMAND ----------

# MAGIC %md ###### Genre Standardisation 2: Split the Genre Column values where a value has "-"

# COMMAND ----------

df_10 = df_9.withColumn("Genre",
  when(col("Genre").isin("One-man show", "Stop-Motion", "Mentalism-Special", "Science-Fiction", "Making-Of", "Science-Fiction-Adventure", "Science-Fiction-Thriller"), array(col("Genre")))
    .otherwise(split(col("Genre"), "-"))).withColumn("Genre", explode(col("Genre"))).orderBy("Genre")

df_11 = df_10.withColumnRenamed("Genre", "New Genre")
display(df_11)

# COMMAND ----------

# MAGIC %md ######Now we have a dimension table of a Standardised Genre mapped to a Title. Next we will LEFT JOIN this table to the original table with the other columns.

# COMMAND ----------

joined_df = df_3.join(df_11, ["Title"], "left")
display(joined_df)

# COMMAND ----------

# MAGIC %md ######Genre Standardisation 3: Split the genres "Science-Fiction-Adventure" and "Science-Fiction-Thriller" into seperate rows of "Science-Fiction", "Adventure" and "Thriller"

# COMMAND ----------

# Define a UDF to split values into two rows if they contain the pattern
def custom_split(genre):
    if genre.startswith("Science-Fiction-"):
        parts = genre.split("-")
        return [f"{parts[0]}-{parts[1]}", parts[2]]
    else:
        return [genre]

# Register the UDF
custom_split_udf = spark.udf.register("custom_split", custom_split, ArrayType(StringType()))

# Apply the UDF and explode the resulting array
final_df = joined_df.withColumn("New Genre", explode(custom_split_udf(col("New Genre"))))
display(final_df)

# COMMAND ----------

# MAGIC %md ######Drop the original Genre Column and rename the "New Genre" column to "Genre"

# COMMAND ----------

# Drop the existing "Genre" column
final_df = final_df.drop("Genre")

# Rename the "New Genre" column to "Genre"
final_df = final_df.withColumnRenamed("New Genre", "Genre")
display(final_df)

# COMMAND ----------

# MAGIC %md ###### Rearrange columns

# COMMAND ----------

# Assuming the DataFrame has columns in the order ['Title', 'Premiere', 'Runtime', 'IMDB Score', 'Language', 'New Genre']
final_df = final_df.select('Title', 'Genre', 'Premiere', 'Runtime', 'IMDB Score', 'Language')
display(final_df)

# COMMAND ----------

# MAGIC %md ###### Save the final cleaned dataframe to Azure Data Lake in a parquet format

# COMMAND ----------

mount_point = "/mnt/demo/RBL/Cleaned/Netflix Originals"

# Define the output path within the mounted directory
output_path = f"{mount_point}"

# Write the DataFrame to CSV in the specified path
final_df.write.parquet(output_path, mode="overwrite")
