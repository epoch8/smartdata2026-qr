import os
import polars as pl
import fsspec


# YC S3 storage options
STORAGE_OPTIONS = {
    "aws_endpoint_url": os.environ["FSSPEC_S3_ENDPOINT_URL"],
    "aws_access_key_id": os.environ["FSSPEC_S3_KEY"],
    "aws_secret_access_key": os.environ["FSSPEC_S3_SECRET"],
    "aws_region": "ru-central-1",
}

BUCKET_RAW = "etl-raw-data"
BUCKET_PROCESSED = "etl-processed-data"

FILEPATH_INPUT = "s3://{BUCKET_RAW}/users/1.parquet"
FILEPATH_OUTPUT = "s3://{BUCKET_PROCESSED}/users/1.parquet"


# Loading
df_pl = pl.scan_parquet(
    source=FILEPATH_INPUT,
    storage_options=STORAGE_OPTIONS,
    hive_partitioning=True,
    retries=5,
).collect()


# Processing
df_pl = df_pl.select(
    [
        pl.col("user_id"),
        pl.col("user_name"),
        pl.col("user_phone"),
    ]
).filter(
    pl.col("user_phone").str.contains("+7")
)


# Saving
with fsspec.open(FILEPATH_OUTPUT, "wb") as f:
    df_pl.write_parquet(f, compression="snappy")
