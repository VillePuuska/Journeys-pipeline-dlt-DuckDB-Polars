import duckdb
import polars as pl
import os
import sys
from utils.format import delay_sec

SOURCE_DB = os.getenv("SOURCE_DB")
SOURCE_SCHEMA = os.getenv("SOURCE_SCHEMA")
SOURCE_TABLE = os.getenv("SOURCE_TABLE")
TARGET_DB = os.getenv("TARGET_DB")
TARGET_SCHEMA = os.getenv("TARGET_SCHEMA")
TARGET_TABLE = os.getenv("TARGET_TABLE")

METADATA_TABLE = "loads"

if None in [
    SOURCE_DB,
    SOURCE_SCHEMA,
    SOURCE_TABLE,
    TARGET_DB,
    TARGET_SCHEMA,
    TARGET_TABLE,
]:
    raise Exception(
        "You must set environment variables $SOURCE_DB, $SOURCE_SCHEMA, "
        + "$SOURCE_TABLE, $TARGET_DB, $TARGET_SCHEMA, and $TARGET_TABLE"
    )

# Get timestamp of the last load from bronze to silver.
with duckdb.connect(TARGET_DB) as db:
    db.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA}")
    db.sql(
        f"CREATE TABLE IF NOT EXISTS {TARGET_SCHEMA}.{METADATA_TABLE} (load_id DOUBLE)"
    )

    last_load = db.sql(
        f"SELECT MAX(load_id) FROM {TARGET_SCHEMA}.{METADATA_TABLE}"
    ).fetchall()[0][0]

    if last_load is None:
        last_load = 0
    else:
        last_load = float(last_load)

print("Latest load:")
print(last_load)
print("*" * 50)

with duckdb.connect(SOURCE_DB) as db:
    source_df = db.sql(
        f"""SELECT *, _dlt_load_id::FLOAT AS load_id
        FROM {SOURCE_SCHEMA}.{SOURCE_TABLE}
        WHERE load_id > {last_load}"""
    ).pl()

print(len(source_df))

if len(source_df) == 0:
    print("No new data to load.")
    sys.exit(0)

# Insert new rows to silver table and the latest load_id to the metadata table in a single transaction.
with duckdb.connect(TARGET_DB) as db:
    db.sql("BEGIN TRANSACTION")
    db.sql(
        f"""INSERT INTO {TARGET_SCHEMA}.{METADATA_TABLE} (load_id)
        VALUES ({source_df["load_id"].max()})"""
    )
    db.sql("COMMIT")
