import polars as pl
import datetime
from utils.format import delay_sec


def transform_bus_data(source_df: pl.DataFrame) -> pl.DataFrame:
    """
    Function that transforms the bronze bus delay dataframe to the silver bus delay dataframe.
    """
    return (
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
            pl.col("monitored_vehicle_journey__origin_aimed_departure_time")
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
            update_time=datetime.datetime.now(),
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
            "update_time",
        ]
    )


def drop_nulls(source_df: pl.DataFrame, logging: bool = False) -> pl.DataFrame:
    """
    Function to drop nulls from a Polars dataframe `source_df`.
    If logging is set to True, then prints all rows with nulls.
    """
    if logging:
        with pl.Config() as cfg:
            cfg.set_tbl_cols(source_df.width)
            nulls = source_df.filter(pl.any_horizontal(pl.all().is_null()))
            if len(nulls) > 0:
                print(f"Dropping {len(nulls)} rows with nulls values:")
                cfg.set_tbl_rows(len(nulls))
                print(nulls)
                print("*" * 50)
    return source_df.drop_nulls()


def deduplicate(
    source_df: pl.DataFrame, pk_cols: list[str], logging: bool = False
) -> pl.DataFrame:
    """
    Function to deduplicate rows in a Polars dataframe `source_df` based on a list of columns `pk_cols`.
    If logging is set to True, then prints all duplicated rows.
    """
    if logging:
        with pl.Config() as cfg:
            cfg.set_tbl_cols(source_df.width)
            duplicates = source_df.filter(pl.struct(pk_cols).is_duplicated())
            if len(duplicates) > 0:
                print(f"Deduplicating {len(duplicates)} rows:")
                cfg.set_tbl_rows(len(duplicates))
                print(duplicates.sort(duplicates.columns))
                print("*" * 50)
    return source_df.unique(pk_cols)
