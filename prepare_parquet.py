# prepare_parquet.py
import os
import pandas as pd
import numpy as np
import warnings

warnings.simplefilter(action="ignore", category=pd.errors.DtypeWarning)

RAW = "data/raw"
PROC = "data/processed"
os.makedirs(PROC, exist_ok=True)

def safe_to_parquet(df, path):
    """Write DataFrame to parquet with a quick sanity pass."""
    print(f"  -> writing {len(df):,} rows to {path} ...")
    df.to_parquet(path, index=False)
    print("     done.")

# 1) Netflix
netflix_csv = os.path.join(RAW, "netflix_titles.csv")
if os.path.exists(netflix_csv):
    print("Converting Netflix…")
    df_netflix = pd.read_csv(netflix_csv)
    safe_to_parquet(df_netflix, os.path.join(PROC, "netflix.parquet"))
else:
    print("Netflix CSV not found at", netflix_csv)

# 2) Movies Metadata (this file often has mixed types; clean before parquet)
movies_meta = os.path.join(RAW, "movies_metadata.csv")
if os.path.exists(movies_meta):
    print("Converting Movies Metadata…")
    # read with low_memory=False to reduce mixed-type sniffing
    df_meta = pd.read_csv(movies_meta, low_memory=False)

    # common problematic fields: runtime, runtimeMinutes, budget, revenue, vote_count, vote_average
    numeric_candidates = ["runtime", "runtimeMinutes", "budget", "revenue", "vote_count", "vote_average"]

    for col in numeric_candidates:
        if col in df_meta.columns:
            print(f"  - coercing column {col} -> numeric (NaN if invalid)")
            # strip commas/spaces and coerce
            df_meta[col] = df_meta[col].replace(r"[,\s]+", "", regex=True)
            df_meta[col] = pd.to_numeric(df_meta[col], errors="coerce")

    # If any column is object but mostly numeric, we can attempt to coerce automatically:
    for col in df_meta.select_dtypes(include="object").columns:
        # small heuristic: if >80% of non-null values look numeric, coerce
        sample = df_meta[col].dropna().astype(str).head(5000)
        if len(sample) == 0:
            continue
        num_like = sample.str.match(r"^-?\d+(\.\d+)?$").sum()
        pct = num_like / len(sample)
        if pct > 0.8:
            try:
                print(f"  - auto coercing mostly-numeric column: {col} (pct numeric ≈ {pct:.2f})")
                df_meta[col] = pd.to_numeric(df_meta[col], errors="coerce")
            except Exception:
                pass

    # Reduce memory: convert some string columns to categories if cardinality low
    for col in ["original_language", "status", "title", "original_title"]:
        if col in df_meta.columns and df_meta[col].nunique(dropna=True) < 2000:
            df_meta[col] = df_meta[col].astype("category")

    safe_to_parquet(df_meta, os.path.join(PROC, "movies_metadata.parquet"))
else:
    print("movies_metadata.csv not found at", movies_meta)

# 3) IMDb basics
imdb_basics = os.path.join(RAW, "title.basics.tsv.gz")
if os.path.exists(imdb_basics):
    print("Converting IMDb basics…")
    # na_values "\N" is an IMDb-specific marker
    df_basics = pd.read_csv(imdb_basics, sep="\t", compression="gzip", na_values="\\N", low_memory=False)

    # imdb basics columns: tconst,titleType,primaryTitle,originalTitle,isAdult,startYear,endYear,runtimeMinutes,genres
    if "runtimeMinutes" in df_basics.columns:
        print("  - coercing imdb runtimeMinutes -> numeric")
        df_basics["runtimeMinutes"] = pd.to_numeric(df_basics["runtimeMinutes"], errors="coerce")

    # convert startYear/endYear to numeric
    for col in ["startYear", "endYear"]:
        if col in df_basics.columns:
            df_basics[col] = pd.to_numeric(df_basics[col], errors="coerce")

    safe_to_parquet(df_basics, os.path.join(PROC, "imdb_basics.parquet"))
else:
    print("IMDb basics not found:", imdb_basics)

# 4) IMDb ratings
imdb_ratings = os.path.join(RAW, "title.ratings.tsv.gz")
if os.path.exists(imdb_ratings):
    print("Converting IMDb ratings…")
    df_ratings = pd.read_csv(imdb_ratings, sep="\t", compression="gzip", na_values="\\N", low_memory=False)
    # ensure rating/votes are numeric
    if "averageRating" in df_ratings.columns:
        df_ratings["averageRating"] = pd.to_numeric(df_ratings["averageRating"], errors="coerce")
    if "numVotes" in df_ratings.columns:
        df_ratings["numVotes"] = pd.to_numeric(df_ratings["numVotes"], errors="coerce")
    safe_to_parquet(df_ratings, os.path.join(PROC, "imdb_ratings.parquet"))
else:
    print("IMDb ratings not found:", imdb_ratings)

print("All done. Parquet files are in", PROC)
