# spark/eda.py  (robust EDA script)
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
import numpy as np

FINAL = "data/final/final_merged.parquet"
OUTDIR = "eda_outputs"
os.makedirs(OUTDIR, exist_ok=True)

print("Loading final dataset...")
df = pd.read_parquet(FINAL)
print("Loaded:", len(df), "rows")

# Helper: safe column accessor with alternatives
def get_col(df, candidates, default=None):
    for c in candidates:
        if c in df.columns:
            return df[c]
    return default

# Standard column names used below (try to find them in df)
avg_rating_col = None
for candidate in ["averageRating", "avg_rating", "imdbRating", "rating"]:
    if candidate in df.columns:
        avg_rating_col = candidate
        break

year_col = None
for candidate in ["startYear", "year", "release_year", "start_year"]:
    if candidate in df.columns:
        year_col = candidate
        break

# genres might be renamed during joins - try common names
genres_col = None
for candidate in ["genres", "meta_genres", "imdb_genres", "genre"]:
    if candidate in df.columns:
        genres_col = candidate
        break

# type (to detect Netflix) may or may not exist
type_col = None
for candidate in ["type", "source", "platform"]:
    if candidate in df.columns:
        type_col = candidate
        break

# 1) Rating distribution
if avg_rating_col is None:
    print("Warning: No rating column found. Skipping rating distribution.")
else:
    ratings = df[avg_rating_col].dropna().astype(float)
    plt.figure(figsize=(8,5))
    sns.histplot(ratings, bins=30, kde=True)
    plt.title("IMDb Rating Distribution")
    plt.xlabel("Rating")
    plt.ylabel("Count")
    plt.tight_layout()
    fname = os.path.join(OUTDIR, "rating_distribution.png")
    plt.savefig(fname)
    plt.close()
    print("Saved:", fname)

# 2) Genre distribution (safe handling)
if genres_col is None:
    print("Warning: No genres column found. Skipping genre distribution.")
else:
    # explode genres (split on commas), drop empty strings, strip whitespace
    all_genres = (
        df[genres_col]
        .fillna("")
        .astype(str)
        .str.split(",")
        .explode()
        .str.strip()
        .replace("", np.nan)
        .dropna()
    )
    counts = all_genres.value_counts()
    top = counts.head(15)
    plt.figure(figsize=(10,6))
    sns.barplot(x=top.values, y=top.index)
    plt.title("Top 15 Genres")
    plt.xlabel("Count")
    plt.tight_layout()
    fname = os.path.join(OUTDIR, "genre_distribution.png")
    plt.savefig(fname)
    plt.close()
    print("Saved:", fname)

# 3) Movies per Year
if year_col is None:
    print("Warning: No year column found. Skipping movies_by_year.")
else:
    years = pd.to_numeric(df[year_col], errors="coerce").dropna().astype(int)
    plt.figure(figsize=(10,5))
    sns.histplot(years, bins=40)
    plt.title("Movies Count Over Years")
    plt.xlabel("Year")
    plt.ylabel("Count")
    plt.tight_layout()
    fname = os.path.join(OUTDIR, "movies_by_year.png")
    plt.savefig(fname)
    plt.close()
    print("Saved:", fname)

# 4) Top 10 highest-rated movies (requires title + rating)
title_col = None
for candidate in ["primaryTitle", "title", "meta_primaryTitle", "name"]:
    if candidate in df.columns:
        title_col = candidate
        break

if avg_rating_col is None or title_col is None:
    print("Warning: Missing title or rating columns. Skipping top10_movies.")
else:
    top10 = df[[title_col, avg_rating_col]].dropna(subset=[avg_rating_col]).sort_values(by=avg_rating_col, ascending=False).head(10)
    plt.figure(figsize=(10,6))
    sns.barplot(x=avg_rating_col, y=title_col, data=top10, palette="viridis")
    plt.title("Top 10 Highest Rated Movies")
    plt.tight_layout()
    fname = os.path.join(OUTDIR, "top10_movies.png")
    plt.savefig(fname)
    plt.close()
    print("Saved:", fname)

# 5) Netflix vs Others ratings (optional; try detect Netflix rows)
# Heuristic: if type_col exists and contains 'Netflix' or if there's a column that came from netflix originally
is_netflix = None
if type_col is not None:
    # create boolean: contains string 'Netflix' (case-insensitive)
    s = df[type_col].astype(str).fillna("")
    if s.str.contains("Netflix", case=False).any():
        is_netflix = s.str.contains("Netflix", case=False).astype(int)
    else:
        # if type column exists but doesn't mention Netflix, skip
        is_netflix = None

# alternative heuristic: some merged rows might have 'show_id' or 'netflix_title' etc.
if is_netflix is None:
    for candidate in ["show_id", "netflix_id", "netflix_title", "netflix"]:
        if candidate in df.columns:
            is_netflix = df[candidate].notna().astype(int)
            break

if is_netflix is None:
    print("Notice: Could not reliably detect Netflix rows. Skipping Netflix vs Others plot.")
else:
    if avg_rating_col is None:
        print("Warning: No rating column for Netflix comparison. Skipping.")
    else:
        plot_df = df.copy()
        plot_df["is_netflix"] = is_netflix
        plt.figure(figsize=(7,5))
        sns.boxplot(x="is_netflix", y=avg_rating_col, data=plot_df)
        plt.xticks([0,1], ["Others", "Netflix"])
        plt.title("Netflix vs Others — Rating Comparison")
        plt.tight_layout()
        fname = os.path.join(OUTDIR, "netflix_vs_others.png")
        plt.savefig(fname)
        plt.close()
        print("Saved:", fname)

print("EDA complete. All generated images (if any) are in:", OUTDIR)
