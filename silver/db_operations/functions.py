import duckdb
import polars as pl

METADATA_TABLE = "loads"
METADATA_TABLE_SCHEMA = "load_id VARCHAR, loaded_rows INTEGER"

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


def create_or_replace_tables(db_path: str, schema: str, table: str):
    """
    Function creates the silver target table and/or metadata tables if they do not exist.

    Params:
        - db_path: filepath to the DuckDB database-file.
        - schema: schema of the silver tables.
        - table: name of the silver table.
    """
    with duckdb.connect(db_path) as db:
        db.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        db.sql(
            f"CREATE TABLE IF NOT EXISTS {schema}.{METADATA_TABLE} (load_id VARCHAR, loaded_rows INTEGER)"
        )
        db.sql(f"CREATE TABLE IF NOT EXISTS {schema}.{table} ({TARGET_TABLE_SCHEMA})")


def get_last_load(db_path: str, schema: str, reset_tables: bool) -> str:
    """
    Function gets the load id of the latest load.

    If there are no previous loads or if reset_tables == True, returns "0".
    This way we load all data from bronze in these cases.

    Params:
        - db_path: filepath to the DuckDB database-file.
        - schema: schema of the silver tables.
    """
    with duckdb.connect(db_path) as db:
        last_load = db.sql(
            f"SELECT MAX(load_id) FROM {schema}.{METADATA_TABLE}"
        ).fetchall()[0][0]

        if last_load is None or reset_tables:
            last_load = "0"

    return last_load


def get_new_data(db_path: str, schema: str, table: str, last_load: str) -> pl.DataFrame:
    """
    Function gets all new rows from the bronze table.

    Params:
        - db_path: filepath to the DuckDB database-file.
        - schema: schema of the silver tables.
        - table: name of the silver table.
        - last_load: load id of the latest load.
    """
    with duckdb.connect(db_path, read_only=True) as db:
        df = db.sql(
            f"""FROM {schema}.{table}
            WHERE _dlt_load_id > {last_load}::VARCHAR"""
        ).pl()

    return df


def insert_new_data(
    df: pl.DataFrame,
    checkpoint: str,
    db_path: str,
    schema: str,
    table: str,
    reset_tables: bool,
):
    """
    Function inserts new rows to the silver table and inserts the new MAX(load id)
    to the metadata/checkpoint table. If reset_tables == True, then also recreates
    the silver tables.

    Everything done in a single transaction.

    Params:
        - df: Polars DataFrame containing new rows.
        - checkpoint: new load id to be inserted to the metadata/checkpoint table.
        - db_path: filepath to the DuckDB database-file.
        - schema: schema of the silver tables.
        - table: name of the silver table.
        - reset_tables: if True, recreate the .
    """
    with duckdb.connect(db_path) as db:
        db.sql("BEGIN TRANSACTION")
        if reset_tables:
            print("Resetting silver tables.")
            print("*" * 50)
            db.sql(
                f"CREATE OR REPLACE TABLE {schema}.{METADATA_TABLE} ({METADATA_TABLE_SCHEMA})"
            )
            db.sql(f"CREATE OR REPLACE TABLE {schema}.{table} ({TARGET_TABLE_SCHEMA})")
        db.sql(
            f"""INSERT INTO {schema}.{METADATA_TABLE} (load_id, loaded_rows)
            VALUES ({checkpoint}, {len(df)})"""
        )
        all_columns = ", ".join(df.columns)
        db.sql(
            f"""INSERT INTO {schema}.{table} ({all_columns})
            FROM df
            ON CONFLICT ({TARGET_TABLE_PK}) DO NOTHING"""
        )
        db.sql("COMMIT")
