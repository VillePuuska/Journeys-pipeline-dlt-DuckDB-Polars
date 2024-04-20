import duckdb
import polars as pl
import os
import sys
from utils.transformations import transform_bus_data, drop_nulls, deduplicate

SOURCE_DB = os.getenv("SOURCE_DB")
SOURCE_SCHEMA = os.getenv("SOURCE_SCHEMA")
SOURCE_TABLE = os.getenv("SOURCE_TABLE")
TARGET_DB = os.getenv("TARGET_DB")
TARGET_SCHEMA = os.getenv("TARGET_SCHEMA")
TARGET_TABLE = os.getenv("TARGET_TABLE")

RESET_TABLES = os.getenv("RESET_TABLES")

METADATA_TABLE = "loads"

TARGET_TABLE_PK = "date, time, line, direction, origin_aimed_departure_time"
TARGET_TABLE_SCHEMA = f"""
date DATE,
time TIME,
line VARCHAR,
operator VARCHAR,
vehicle VARCHAR,
journey_pattern VARCHAR,
origin_short_name VARCHAR,
destination_short_name VARCHAR,
direction VARCHAR,
longitude DOUBLE,
latitude DOUBLE,
speed REAL,
origin_aimed_departure_time TIME,
delay BIGINT,
update_time TIMESTAMPTZ,
PRIMARY KEY ({TARGET_TABLE_PK})
"""

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
        f"CREATE TABLE IF NOT EXISTS {TARGET_SCHEMA}.{METADATA_TABLE} (load_id VARCHAR, loaded_rows INTEGER)"
    )
    db.sql(
        f"CREATE TABLE IF NOT EXISTS {TARGET_SCHEMA}.{TARGET_TABLE} ({TARGET_TABLE_SCHEMA})"
    )

    last_load = db.sql(
        f"SELECT MAX(load_id) FROM {TARGET_SCHEMA}.{METADATA_TABLE}"
    ).fetchall()[0][0]

    # If we're resetting the tables or the metadata table did not yet exist,
    # set last_load to "0" to load all data.
    if last_load is None or (
        RESET_TABLES is not None and RESET_TABLES.lower() == "true"
    ):
        last_load = "0"

print("Latest load:")
print(last_load)
print("*" * 50)

with duckdb.connect(SOURCE_DB, read_only=True) as db:
    source_df = db.sql(
        f"""FROM {SOURCE_SCHEMA}.{SOURCE_TABLE}
        WHERE _dlt_load_id > {last_load}::VARCHAR"""
    ).pl()

if len(source_df) == 0:
    print("No new data to load.")
    sys.exit(0)

# Transform the bronze data to silver format.
new_df = transform_bus_data(source_df=source_df)

# Drop and log rows with nulls.
new_df = drop_nulls(source_df=new_df, logging=True)

# Deduplicate rows by PK.
pk_cols = TARGET_TABLE_PK.split(", ")
new_df = deduplicate(source_df=new_df, pk_cols=pk_cols, logging=True)

new_row_count = len(new_df)
print(f"# of new rows: {new_row_count}")
print("*" * 50)

with pl.Config() as cfg:
    cfg.set_tbl_cols(new_df.width)
    print(new_df)

# Reset/recreate the tables if required.
# Insert new rows to silver table.
# Insert latest load_id and number of rows in dataframe to the metadata table.
# Everything is done in a single transaction.
# NOTE: logged number of rows might not be the number of new rows since the dataframe
# is merged into the existing table. There might be rows dropped/deduplicated.
with duckdb.connect(TARGET_DB) as db:
    db.sql("BEGIN TRANSACTION")
    # If we're resetting tables, we recreate the tables inside the transaction.
    if RESET_TABLES is not None and RESET_TABLES.lower() == "true":
        print("Resetting silver tables.")
        print("*" * 50)
        db.sql(
            f"CREATE OR REPLACE TABLE {TARGET_SCHEMA}.{METADATA_TABLE} (load_id VARCHAR, loaded_rows INTEGER)"
        )
        db.sql(
            f"CREATE OR REPLACE TABLE {TARGET_SCHEMA}.{TARGET_TABLE} ({TARGET_TABLE_SCHEMA})"
        )
    db.sql(
        f"""INSERT INTO {TARGET_SCHEMA}.{METADATA_TABLE} (load_id, loaded_rows)
        VALUES ({source_df["_dlt_load_id"].max()}, {new_row_count})"""
    )
    all_columns = ", ".join(new_df.columns)
    db.sql(
        f"""INSERT INTO {TARGET_SCHEMA}.{TARGET_TABLE} ({all_columns})
        FROM new_df
        ON CONFLICT ({TARGET_TABLE_PK}) DO NOTHING"""
    )
    db.sql("COMMIT")
