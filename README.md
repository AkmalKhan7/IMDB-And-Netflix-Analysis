IMDB & Netflix Analysis — Project README
Authors: Akmal Khan & Geedhabalan Devaraj
Course: CSC1142 — Data Engineering / Big Data Analytics

Quick summary
This project builds an end‑to‑end ETL + EDA pipeline that merges Netflix, TMDB and IMDb datasets using PySpark, creates a final merged Parquet dataset, and runs Exploratory Data Analysis (EDA) with Python (Pandas / Seaborn).
Repository contains code only (no datasets). See Dataset download below.

Datasets (download BEFORE running)
You must download these datasets and place them into the data/raw/ folder (create if missing).

Netflix Titles dataset (Kaggle) — Netflix movies/tv shows
https://www.kaggle.com/datasets/shivamb/netflix-shows
→ place as data/raw/netflix_titles.csv or similar (see note below)

IMDb basics — title.basics.tsv.gz (official IMDb datasets)
https://datasets.imdbws.com/title.basics.tsv.gz
→ place as data/raw/title.basics.tsv.gz

IMDb ratings — title.ratings.tsv.gz (official IMDb datasets)
https://datasets.imdbws.com/title.ratings.tsv.gz
→ place as data/raw/title.ratings.tsv.gz

TMDB Movies Metadata (Kaggle) — movies_metadata.csv
https://www.kaggle.com/datasets/rounakbanik/the-movies-dataset
→ place as data/raw/movies_metadata.csv

Filename flexibility: if filenames differ, update extract.py paths or move/rename files to the names above.

Note about repo contents
This GitHub contains only code (no raw datasets, no generated outputs).

If you previously uploaded any files in this chat, some have expired; re‑upload if you want us to re‑read any attachments.

Recommended environment
Java JDK 17 (Temurin / Adoptium recommended) — required by Spark.

Python 3.8+ (Python 3.10 or 3.13 used successfully here).

A virtual environment (venv) is strongly recommended.

Setup — Step‑by‑step
1) Install Java (JDK 17)
Temurin MSI (Windows) or brew install --cask temurin17 (macOS).

Set JAVA_HOME to the JDK folder and add %JAVA_HOME%\bin (Windows) or add export JAVA_HOME="/Library/Java/JavaVirtualMachines/temurin-17.jdk/Contents/Home" (macOS).

Verify:

java -version
# expected: openjdk version "17.x"
2) Create & activate Python venv
macOS / Linux

python3 -m venv venv
source venv/bin/activate
Windows (PowerShell)

python -m venv venv
.\venv\Scripts\Activate.ps1
# If execution policy blocks, run as Admin:
# Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
3) Install Python packages
pip install --upgrade pip
pip install pyspark pandas pyarrow fastparquet matplotlib seaborn
(You can add other packages if needed — these are the ones used by the project.)

How to place datasets in the repo
Create directories and move files:

mkdir -p data/raw data/processed data/final eda_outputs
# Move the downloaded files into data/raw/
# Example:
mv ~/Downloads/netflix_titles.csv data/raw/netflix_titles.csv
mv ~/Downloads/title.basics.tsv.gz data/raw/title.basics.tsv.gz
mv ~/Downloads/title.ratings.tsv.gz data/raw/title.ratings.tsv.gz
mv ~/Downloads/movies_metadata.csv data/raw/movies_metadata.csv
Running the pipeline — exact order
Run all commands from the project root (where spark/ folder and extract.py exist).

1) Extract (create processed Parquet files)
# using venv python
python3 spark/extract.py
extract.py reads raw files from data/raw/ and writes processed Parquet files into data/processed/.

After success you should see files such as:

data/processed/netflix.parquet

data/processed/movies_metadata.parquet

data/processed/imdb_basics.parquet

data/processed/imdb_ratings.parquet

2) Transform (merge datasets into final dataset)
python3 spark/transform.py
transform.py loads parquet files from data/processed/, performs joins, resolves column collisions, and writes:

data/final/final_merged.parquet

If you run into Java heap OOM errors: open spark/transform.py and adjust DRIVER_MEM (e.g. "4g" or "8g") and spark.sql.shuffle.partitions (e.g. "8").

3) Exploratory Data Analysis (EDA)
python3 spark/eda.py
spark/eda.py reads data/final/final_merged.parquet and creates .png images in eda_outputs/:

rating_distribution.png

genre_distribution.png

movies_by_year.png

top10_movies.png

netflix_vs_others.png

Expected outputs & locations
Processed parquet: data/processed/*.parquet

Final merged dataset: data/final/final_merged.parquet

EDA images: eda_outputs/*.png

Presentation PPT / PDF: you can produce from the presentation/ (if added) or use the provided slide content.

How to reproduce the presentation PDF (examiner)
Generate EDA images (run spark/eda.py).

Open presentation_template.pptx (if included) or use the slide content in the repo to create slides.

Insert images from eda_outputs/ into slide placeholders.

Export to PDF from PowerPoint / Google Slides → File → Export → Create PDF.

Troubleshooting (common issues)
java not found
Ensure JDK installed and JAVA_HOME is set and %JAVA_HOME%\bin is in PATH. Restart terminal after setting env vars.

ModuleNotFoundError: No module named 'pyspark'
Activate venv and run: pip install pyspark

Make sure you run the script with the same Python as the venv: which python3 should point to venv/bin/python3.

PySpark says Missing Python executable 'python3' or similar
Set Python for PySpark in the session (PowerShell example):

$py = (Get-Command python).Source
$env:PYSPARK_PYTHON = $py
$env:PYSPARK_DRIVER_PYTHON = $py
OutOfMemoryError / Java heap space
Increase Spark driver memory in transform.py:

.config("spark.driver.memory", "4g")
.config("spark.executor.memory", "4g")
.config("spark.sql.shuffle.partitions", "8")
Or run on a machine with more RAM or on cloud (Colab / Databricks).

Column name conflicts (e.g., COLUMN_ALREADY_EXISTS)
transform.py contains logic to rename conflicting columns; ensure you are using the latest version in this repo.

Notes to the examiner
Datasets are not stored in the GitHub repo due to size and copyright — please download the four public datasets above and place them into data/raw/ before running.

The code was tested on macOS and Windows with Python 3.10/3.13 and JDK 17.

If you prefer not to install Java & Spark locally, we can provide a Colab notebook version that runs the entire pipeline in the cloud; request if desired.

Contact (project team)
Akmal Khan — akmalkhan6394

Geedhabalan Devaraj — geedhabalan007

License / Acknowledgements
Datasets from Kaggle and IMDb (official dataset site). See in‑repo DATA_SOURCES.md for exact links and citation formats.

End of README
