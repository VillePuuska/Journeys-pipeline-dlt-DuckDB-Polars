import polars as pl
import os
import sys
from db_operations.functions import (
    create_or_replace_tables,
    get_last_load,
    get_new_data,
    insert_new_data,
    TARGET_TABLE_PK,
)
from utils.transformations import transform_bus_data, drop_nulls, deduplicate

SOURCE_DB = os.getenv("SOURCE_DB")
SOURCE_SCHEMA = os.getenv("SOURCE_SCHEMA")
SOURCE_TABLE = os.getenv("SOURCE_TABLE")
TARGET_DB = os.getenv("TARGET_DB")
TARGET_SCHEMA = os.getenv("TARGET_SCHEMA")
TARGET_TABLE = os.getenv("TARGET_TABLE")

RESET_TABLES = os.getenv("RESET_TABLES")
if RESET_TABLES is not None and RESET_TABLES.lower() == "true":
    RESET_TABLES = True
else:
    RESET_TABLES = False

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

# Create the silver table and metadata/checkpoint table if they do not exist.
create_or_replace_tables(db_path=TARGET_DB, schema=TARGET_SCHEMA, table=TARGET_TABLE)

# Get the load id of the latest silver load.
last_load = get_last_load(
    db_path=TARGET_DB, schema=TARGET_SCHEMA, reset_tables=RESET_TABLES
)
print("Latest load:")
print(last_load)
print("*" * 50)

# Get all new rows from bronze table.
source_df = get_new_data(
    db_path=SOURCE_DB, schema=SOURCE_SCHEMA, table=SOURCE_TABLE, last_load=last_load
)
if len(source_df) == 0:
    print("No new data to load.")
    sys.exit(0)

# Transform the bronze data to silver format.
new_df = transform_bus_data(source_df=source_df)

# Drop and log rows with nulls.
new_df = drop_nulls(source_df=new_df, logging=True)

# Deduplicate rows by PK.
# NOTE: TARGET_TABLE_PK is defined in submodule db_operations.
pk_cols = TARGET_TABLE_PK.split(", ")
new_df = deduplicate(source_df=new_df, pk_cols=pk_cols, logging=True)

print(f"# of new rows: {len(new_df)}")
print("*" * 50)

with pl.Config() as cfg:
    cfg.set_tbl_cols(new_df.width)
    print(new_df)

# Insert new data to silver. Reset tables if required.
insert_new_data(
    df=new_df,
    checkpoint=source_df["_dlt_load_id"].max(),
    db_path=TARGET_DB,
    schema=TARGET_SCHEMA,
    table=TARGET_TABLE,
    reset_tables=RESET_TABLES,
)
