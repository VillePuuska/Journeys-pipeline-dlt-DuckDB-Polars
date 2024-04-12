import duckdb
import polars as pl
import os
import sys
import deltalake
import datetime

SOURCE_DB = os.getenv("SOURCE_DB")
SOURCE_SCHEMA = os.getenv("SOURCE_SCHEMA")
SOURCE_TABLE = os.getenv("SOURCE_TABLE")
TARGET_DIR = os.getenv("TARGET_DIR")

if None in [
    SOURCE_DB,
    SOURCE_SCHEMA,
    SOURCE_TABLE,
    TARGET_DIR,
]:
    raise Exception(
        "You must set environment variables $SOURCE_DB, $SOURCE_SCHEMA, "
        + "$SOURCE_TABLE, and $TARGET_DIR"
    )

TARGET_TABLE = "journeys_data"
TARGET_PATH = os.path.join(TARGET_DIR, TARGET_TABLE)

try:
    max_update = pl.read_delta(TARGET_PATH).select("update_time").max().item()
except FileNotFoundError:
    max_update = datetime.datetime.fromisoformat("1970-01-01 00:00:00.000")
except deltalake._internal.TableNotFoundError:
    max_update = datetime.datetime.fromisoformat("1970-01-01 00:00:00.000")

with duckdb.connect(SOURCE_DB, read_only=True) as db:
    source_df = (
        db.sql(
            f"FROM {SOURCE_SCHEMA}.{SOURCE_TABLE} WHERE update_time > '{max_update}'"
        )
        .pl()
        .cast({"time": pl.String, "origin_aimed_departure_time": pl.String})
    )

if len(source_df) == 0:
    print("No new data.")
    sys.exit(0)

with pl.Config() as cfg:
    cfg.set_tbl_cols(source_df.width)
    print("Loading new data from silver:")
    print(source_df)
    print("*" * 50)

try:
    (
        source_df.write_delta(
            TARGET_PATH,
            mode="merge",
            delta_merge_options={
                "source_alias": "s",
                "target_alias": "t",
                "predicate": """
                s.date = t.date AND
                s.time = t.time AND
                s.line = t.line AND
                s.direction = t.direction AND
                s.origin_aimed_departure_time = t.origin_aimed_departure_time
                """,
            },
        )
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute()
    )
except deltalake._internal.TableNotFoundError:
    source_df.write_delta(
        TARGET_PATH,
        mode="overwrite",
    )

row_count = pl.read_delta(TARGET_PATH).select("date").count().item()
print(f"New row count: {row_count}")
