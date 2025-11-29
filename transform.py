# spark/transform.py  -- updated to avoid "COLUMN_ALREADY_EXISTS" and reduce OOM risk
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim
from pyspark.sql import DataFrame

# ====== CONFIG ======
DRIVER_MEM = "8g"                # adjust to your physical RAM (8g is good for 16GB machines)
SHUFFLE_PARTITIONS = "8"         # reduce for laptops
BROADCAST_THRESHOLD = 200000     # heuristic row count to decide broadcasting
# ====================

spark = (
    SparkSession.builder
    .appName("transform_movies")
    .config("spark.driver.memory", DRIVER_MEM)
    .config("spark.executor.memory", DRIVER_MEM)
    .config("spark.sql.shuffle.partitions", SHUFFLE_PARTITIONS)
    .config("spark.sql.execution.arrow.pyspark.enabled", "true")
    .getOrCreate()
)

print("Spark config:")
print(" driver.memory =", spark.sparkContext.getConf().get("spark.driver.memory"))
print(" shuffle.partitions =", spark.sparkContext.getConf().get("spark.sql.shuffle.partitions"))

# Helper: rename columns in `df` that conflict with columns in `other`
def rename_conflicting_columns(df: DataFrame, other: DataFrame, prefix: str, keep_keys=()):
    other_cols = set(other.columns)
    for c in df.columns:
        if c in other_cols and c not in keep_keys:
            df = df.withColumnRenamed(c, f"{prefix}{c}")
    return df

# Helper: broadcast if small
from pyspark.sql.functions import broadcast as spark_broadcast
def broadcast_if_small(df: DataFrame, threshold=BROADCAST_THRESHOLD):
    try:
        cnt = df.count()
    except Exception:
        cnt = None
    if cnt is not None and cnt <= threshold:
        return spark_broadcast(df)
    return df

# Load preprocessed parquet files
netflix = spark.read.parquet("data/processed/netflix.parquet")
meta = spark.read.parquet("data/processed/movies_metadata.parquet")
basics = spark.read.parquet("data/processed/imdb_basics.parquet")
ratings = spark.read.parquet("data/processed/imdb_ratings.parquet")

print("Loaded parquet files.")
print(f"Counts -> netflix: {netflix.count()}, meta: {meta.count()}, basics: {basics.count()}, ratings: {ratings.count()}")

# CLEAN TITLE FIELDS (common join key)
netflix = netflix.withColumn("title_clean", lower(trim(col("title"))))
meta = meta.withColumn("title_clean", lower(trim(col("title"))))
basics = basics.withColumn("title_clean", lower(trim(col("primaryTitle"))))

# Before any joins: avoid duplicate column names by renaming columns in the larger tables
# We will keep 'title_clean' as the join key, so exclude it from renaming.
keep_keys = ("title_clean",)

# Rename columns in meta and basics that would collide with netflix
meta = rename_conflicting_columns(meta, netflix, prefix="meta_", keep_keys=keep_keys)
basics = rename_conflicting_columns(basics, netflix, prefix="imdb_", keep_keys=keep_keys)
# Also rename columns in basics that would collide with meta (avoid later collisions)
basics = rename_conflicting_columns(basics, meta, prefix="imdb_", keep_keys=keep_keys)

# JOIN IMDb BASICS + RATINGS on tconst — avoid collisions here too
# rename ratings columns if they conflict with basics (except tconst)
ratings = rename_conflicting_columns(ratings, basics, prefix="ratings_", keep_keys=("tconst",))

imdb_full = basics.join(ratings, on="tconst", how="left")
print("Joined basics + ratings into imdb_full.")

# Broadcast small table if appropriate (netflix is small here)
netflix_small = bestseller = netflix  # keep variable name short
netflix_b = broadcast_if_small(netflix_small, threshold=BROADCAST_THRESHOLD)

# Join meta and netflix:
# To preserve Netflix rows (left semantics) we do netflix LEFT JOIN meta.
# Because we've renamed meta columns with "meta_" prefix, collisions are avoided.
merged_1 = netflix_b.join(meta, on="title_clean", how="left")
print("Joined netflix -> meta (merged_1).")

# Now join merged_1 with imdb_full on title_clean.
# imdb_full may not have title_clean as key; if not, join on title_clean will use imdb's renamed field.
# We already renamed imdb fields to avoid name collisions.
final_df = merged_1.join(imdb_full, on="title_clean", how="left")
print("Joined merged_1 -> imdb_full (final_df created).")

# Repartition before write to avoid too many small files but not a single file
final_df = final_df.repartition(int(SHUFFLE_PARTITIONS))

out_path = "data/final/final_merged.parquet"
final_df.write.mode("overwrite").parquet(out_path)

print(f"Final merged dataset saved to {out_path}")
spark.stop()
