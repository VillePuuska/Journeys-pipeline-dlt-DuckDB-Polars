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

    # If we're resetting tables, we recreate the tables and we set the env variable to false afterwards.
    if RESET_TABLES is not None and RESET_TABLES.lower() == "true":
        print("Resetting silver tables.")
        print("*" * 50)
        db.sql(
            f"CREATE OR REPLACE TABLE {TARGET_SCHEMA}.{METADATA_TABLE} (load_id VARCHAR, loaded_rows INTEGER)"
        )
        db.sql(
            f"CREATE OR REPLACE TABLE {TARGET_SCHEMA}.{TARGET_TABLE} ({TARGET_TABLE_SCHEMA})"
        )
    else:
        db.sql(
            f"CREATE TABLE IF NOT EXISTS {TARGET_SCHEMA}.{METADATA_TABLE} (load_id VARCHAR, loaded_rows INTEGER)"
        )
        db.sql(
            f"CREATE TABLE IF NOT EXISTS {TARGET_SCHEMA}.{TARGET_TABLE} ({TARGET_TABLE_SCHEMA})"
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
        f"""FROM {SOURCE_SCHEMA}.{SOURCE_TABLE}
        WHERE _dlt_load_id > {last_load}"""
    ).pl()

if len(source_df) == 0:
    print("No new data to load.")
    sys.exit(0)

new_df = (
    source_df.rename(
        {
            "monitored_vehicle_journey__line_ref": "line",
            "monitored_vehicle_journey__operator_ref": "operator",
            "monitored_vehicle_journey__vehicle_ref": "vehicle",
            "monitored_vehicle_journey__journey_pattern_ref": "journey_pattern",
            "monitored_vehicle_journey__origin_short_name": "origin_short_name",
            "monitored_vehicle_journey__destination_short_name": "destination_short_name",
            "monitored_vehicle_journey__direction_ref": "direction",
        }
    ).with_columns(
        pl.col("recorded_at_time").cast(pl.Date).alias("date"),
        pl.col("recorded_at_time").cast(pl.Time).alias("time"),
        pl.col("monitored_vehicle_journey__vehicle_location__longitude")
        .cast(pl.Float64)
        .alias("longitude"),
        pl.col("monitored_vehicle_journey__vehicle_location__latitude")
        .cast(pl.Float64)
        .alias("latitude"),
        pl.col("monitored_vehicle_journey__speed").cast(pl.Float32).alias("speed"),
        (
            pl.col("monitored_vehicle_journey__origin_aimed_departure_time").str.slice(
                0, 2
            )
            + pl.col(
                "monitored_vehicle_journey__origin_aimed_departure_time"
            ).str.slice(2)
        )
        .str.strptime(dtype=pl.Time, format="%H%M")
        .alias("origin_aimed_departure_time"),
        pl.struct("monitored_vehicle_journey__delay")
        .map_batches(
            lambda x: pl.Series(
                map(delay_sec, x.struct.field("monitored_vehicle_journey__delay"))
            ),
            return_dtype=pl.Int64,
        )
        .alias("delay"),
    )
).select(
    [
        "date",
        "time",
        "line",
        "operator",
        "vehicle",
        "journey_pattern",
        "origin_short_name",
        "destination_short_name",
        "direction",
        "longitude",
        "latitude",
        "speed",
        "origin_aimed_departure_time",
        "delay",
    ]
)

# Deduplicate new data and drop nulls.
new_df = new_df.unique(TARGET_TABLE_PK.split(", ")).drop_nulls()

new_row_count = len(new_df)
print(f"# of new rows: {new_row_count}")
print("*" * 50)

print(new_df)

# Insert new rows to silver table and the latest load_id to the metadata table in a single transaction.
with duckdb.connect(TARGET_DB) as db:
    db.sql("BEGIN TRANSACTION")
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
