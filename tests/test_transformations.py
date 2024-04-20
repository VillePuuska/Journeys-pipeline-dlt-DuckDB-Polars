from utils.transformations import transform_bus_data, drop_nulls, deduplicate
import polars as pl
import datetime


data = {
    "recorded_at_time": [
        datetime.datetime.fromisoformat("2014-10-05T23:19:00.008"),
        None,
        datetime.datetime.fromisoformat("2014-12-07T23:19:02.008"),
        datetime.datetime.fromisoformat("2014-01-08T23:19:03.008"),
        datetime.datetime.fromisoformat("2014-02-09T23:19:04.008"),
        datetime.datetime.fromisoformat("2014-02-09T23:19:04.008"),
    ],
    "valid_until_time": [
        datetime.datetime.fromisoformat("2014-10-05T23:19:30.008"),
        datetime.datetime.fromisoformat("2014-11-06T23:19:31.008"),
        datetime.datetime.fromisoformat("2014-12-07T23:19:32.008"),
        datetime.datetime.fromisoformat("2014-01-08T23:19:33.008"),
        datetime.datetime.fromisoformat("2014-02-09T23:19:34.008"),
        datetime.datetime.fromisoformat("2014-02-09T23:19:34.008"),
    ],
    "monitored_vehicle_journey__line_ref": [
        "3",
        "3A",
        "15B",
        "1",
        "70",
        "70",
    ],
    "monitored_vehicle_journey__direction_ref": [
        "1",
        "1",
        "1",
        "2",
        "2",
        "2",
    ],
    "monitored_vehicle_journey__framed_vehicle_journey_ref__date_frame_ref": [
        "2014-10-05",
        "2014-11-06",
        "2014-12-07",
        "2014-01-08",
        "2014-02-09",
        "2014-02-09",
    ],
    "monitored_vehicle_journey__framed_vehicle_journey_ref__dated_vehicle_journey_ref": [
        "http://data.itsfactory.fi/journeys/api/1/journeys/3_2240_1028_3615",
        "http://data.itsfactory.fi/journeys/api/1/journeys/3_2240_1028_3615",
        "http://data.itsfactory.fi/journeys/api/1/journeys/3_2240_1028_3615",
        "http://data.itsfactory.fi/journeys/api/1/journeys/3_2240_1028_3615",
        "http://data.itsfactory.fi/journeys/api/1/journeys/3_2240_1028_3615",
        "http://data.itsfactory.fi/journeys/api/1/journeys/3_2240_1028_3615",
    ],
    "monitored_vehicle_journey__vehicle_location__longitude": [
        "23.6904222",
        "23.7904222",
        "23.8904222",
        "23.9904222",
        "24.0904222",
        "24.0904222",
    ],
    "monitored_vehicle_journey__vehicle_location__latitude": [
        "61.5267588",
        "60.5267588",
        "59.5267588",
        "58.5267588",
        "57.5267588",
        "57.5267588",
    ],
    "monitored_vehicle_journey__operator_ref": [
        "TKL",
        "TKL",
        "ASD",
        "DSA",
        "Y",
        "Y",
    ],
    "monitored_vehicle_journey__bearing": [
        "92.0",
        "1.0",
        "2.0",
        "3.1",
        "123.2",
        "123.2",
    ],
    "monitored_vehicle_journey__delay": [
        "-P0Y0M0DT0H3M20.000S",
        "P0Y0M0DT0H10M21.000S",
        "-P0Y0M0DT0H4M12.000S",
        "P0Y0M0DT0H10M20.000S",
        "P0Y0M0DT0H0M00.000S",
        "P0Y0M0DT0H0M00.000S",
    ],
    "monitored_vehicle_journey__vehicle_ref": [
        "TKL_028",
        "TKL_123",
        "TKL_XXX",
        "TKL_YYY",
        "TKL_028",
        "TKL_028",
    ],
    "monitored_vehicle_journey__journey_pattern_ref": [
        "3V",
        "3Z",
        "15X",
        "1V",
        "70W",
        "70W",
    ],
    "monitored_vehicle_journey__origin_short_name": [
        "3615",
        "3616",
        None,
        "3715",
        "0615",
        "0615",
    ],
    "monitored_vehicle_journey__destination_short_name": [
        "1028",
        "1128",
        "1038",
        "1029",
        "0028",
        "0028",
    ],
    "monitored_vehicle_journey__speed": [
        "10.0",
        "21.0",
        "32.0",
        "43.0",
        "55.5",
        "55.5",
    ],
    "monitored_vehicle_journey__origin_aimed_departure_time": [
        "2240",
        "2140",
        "1200",
        "2005",
        "1234",
        "1234",
    ],
    "_dlt_load_id": [
        "1713601219.3070812",
        "1713601219.3070812",
        "1713601319.3070812",
        "1713601319.3070812",
        "1713602219.3070812",
        "1713602219.3070812",
    ],
    "_dlt_id": [
        "l3PGkQanjz8xGQ",
        "asdgkQanjz8xGQ",
        "l3PGdr65jz8xGQ",
        "l3PGkQanac4XGQ",
        "dtPGkQanjz8xZZ",
        "dtPGkQanjz8xZZ",
    ],
}
schema = {
    "recorded_at_time": pl.Datetime,
    "valid_until_time": pl.Datetime,
    "monitored_vehicle_journey__line_ref": pl.Utf8,
    "monitored_vehicle_journey__direction_ref": pl.Utf8,
    "monitored_vehicle_journey__framed_vehicle_journey_ref__date_frame_ref": pl.Utf8,
    "monitored_vehicle_journey__framed_vehicle_journey_ref__dated_vehicle_journey_ref": pl.Utf8,
    "monitored_vehicle_journey__vehicle_location__longitude": pl.Utf8,
    "monitored_vehicle_journey__vehicle_location__latitude": pl.Utf8,
    "monitored_vehicle_journey__operator_ref": pl.Utf8,
    "monitored_vehicle_journey__bearing": pl.Utf8,
    "monitored_vehicle_journey__delay": pl.Utf8,
    "monitored_vehicle_journey__vehicle_ref": pl.Utf8,
    "monitored_vehicle_journey__journey_pattern_ref": pl.Utf8,
    "monitored_vehicle_journey__origin_short_name": pl.Utf8,
    "monitored_vehicle_journey__destination_short_name": pl.Utf8,
    "monitored_vehicle_journey__speed": pl.Utf8,
    "monitored_vehicle_journey__origin_aimed_departure_time": pl.Utf8,
    "_dlt_load_id": pl.Utf8,
    "_dlt_id": pl.Utf8,
}

df_test = pl.DataFrame(data=data, schema=schema)
df_transform_bus_data = transform_bus_data(df_test)
df_drop_nulls = drop_nulls(df_transform_bus_data)
df_deduplicate = deduplicate(
    df_drop_nulls, ["date", "time", "line", "direction", "origin_aimed_departure_time"]
)


def test_transform_bus_data():
    assert df_transform_bus_data.columns == [
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
    assert df_transform_bus_data.dtypes == [
        pl.Date,
        pl.Time,
        pl.Utf8,
        pl.Utf8,
        pl.Utf8,
        pl.Utf8,
        pl.Utf8,
        pl.Utf8,
        pl.Utf8,
        pl.Float64,
        pl.Float64,
        pl.Float32,
        pl.Time,
        pl.Int64,
        pl.Datetime,
    ]
    assert df_transform_bus_data.row(0)[:-1] == (
        datetime.date(2014, 10, 5),
        datetime.time(23, 19, 0, 8000),
        "3",
        "TKL",
        "TKL_028",
        "3V",
        "3615",
        "1028",
        "1",
        23.6904222,
        61.5267588,
        10.0,
        datetime.time(22, 40),
        -200,
    )
    assert df_transform_bus_data.row(1)[:-1] == (
        None,
        None,
        "3A",
        "TKL",
        "TKL_123",
        "3Z",
        "3616",
        "1128",
        "1",
        23.7904222,
        60.5267588,
        21.0,
        datetime.time(21, 40),
        621,
    )
    assert len(df_transform_bus_data) == 6


def test_drop_nulls():
    assert len(df_drop_nulls) == 4


def test_deduplicate():
    assert len(df_deduplicate) == 3
