import duckdb
import polars as pl
import os
import sys
import deltalake

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

with duckdb.connect(SOURCE_DB) as db:
    source_df = (
        db.sql(f"FROM {SOURCE_SCHEMA}.{SOURCE_TABLE}")
        .pl()
        .cast({"time": pl.String, "origin_aimed_departure_time": pl.String})
    )

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


print(pl.read_delta(TARGET_PATH).count())
