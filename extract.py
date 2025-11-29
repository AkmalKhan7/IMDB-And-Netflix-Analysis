from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("extract_movies").getOrCreate()

netflix = spark.read.parquet("data/processed/netflix.parquet")
meta = spark.read.parquet("data/processed/movies_metadata.parquet")
basics = spark.read.parquet("data/processed/imdb_basics.parquet")
ratings = spark.read.parquet("data/processed/imdb_ratings.parquet")

print("Netflix:", netflix.count(), "rows")
print("Movies Meta:", meta.count(), "rows")
print("IMDB basics:", basics.count(), "rows")
print("IMDB ratings:", ratings.count(), "rows")

spark.stop()
