#  **Import Libraries and Load the Data**

from pyspark.sql import SparkSession # required to created a dataframe
spark=SparkSession.builder.appName("Basics").getOrCreate() 

import pandas as pd
import numpy as np
import time
from  pyspark.sql.functions import *
import pyspark.sql.types
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.types import DoubleType



df = spark.read.csv("../Data/NetflixOriginals.csv",
                   header = True)


#  *Deeeping on the dataset **
print("Below the dataset Schema:")
df.printSchema()
print("------- --------  ----------")

print("Below a dataset frame:")
df.show(10)

print("------- --------  ----------")

print("Selecting by 'Title' and 'Genre': ")
df.select("Title", "Genre").show(5)

print("------- --------  ----------")

print("Creating new column named 'Movie Hour': ")
df = df.withColumn('Movie_Hour', 
                   round(df.Runtime/60, 2))
df.show(5)

print("------- --------  ----------")


print("Filtering by 'Genre Horror' with previous column created: ")
df_horror = df.filter(df.Genre == "Horror")
df_horror.show(10)

print("------- --------  ----------")


print("Filtering by 'Genre Horror' and 'English language' with previous column created: ")
df_language_genre = df.filter((df.Genre.startswith('Horror')) & (df.Language == "English"))
df_language_genre.show(10)

print("------- --------  ----------")


print("Droping some dataset columns:")
drop_df = df.drop(df.Runtime)
drop_df.show(10)

drop_df_2 = df.drop("Runtime", "IMDB Score")
drop_df_2.show(10)

print("------- --------  ----------")


print("# **Data Merging and Data Agg. Using PySpark SQL** [mean] [count] [sum] [min] [max] ")

print("------- --------  ----------")

print("Genre Hour mean:")
df_mean_moviehour_genre = df.groupby(["Genre"]).agg(round(mean('Movie_Hour'),2))
df_mean_moviehour_genre.show()

print("------- --------  ----------")

print("Genre Hour + Language mean:")
df_mean_moviehour_genre_lang = df.groupby(["Genre", "Language"]).mean("Movie_Hour")
df_mean_moviehour_genre_lang.show()

print("------- --------  ----------")

print("Title + Movie H:")
df_max_time_movie = df.groupby("Title").max("Movie_Hour")
df_max_time_movie.show()