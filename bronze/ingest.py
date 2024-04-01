import dlt
from dlt.sources.helpers import requests
from dlt.pipeline.exceptions import PipelineStepFailed
import time

# JourneysAPI Vehicle Activity endpoint does not use pagination
# and the amount of data is relatively low so we can just use the
# response body directly instead of making a generator or a DltResource.
response = requests.get(
    "http://data.itsfactory.fi/journeys/api/1/vehicle-activity",
    headers={"User-Agent": "Just using the API to test dlt"},
)
response.raise_for_status()

pipeline = dlt.pipeline(
    pipeline_name="ingest_pipe",
    pipelines_dir="pipes",
    destination="duckdb",
    dataset_name="bronze",
)

success = False
for i in range(3):
    try:
        load_info = pipeline.run(
            data=response.json()["body"],
            table_name="journeys_data",
        )
        success = True
        break
    except PipelineStepFailed as e:
        print(
            f"Pipeline failed. Maybe DuckDB is locked? Waiting and trying again. Try #{i+1}."
        )
        print("Actual error message:")
        print("-" * 50)
        print(e)
        print("-" * 50)
        print()
        time.sleep(5.0)

if not success:
    pipeline_fallback = dlt.pipeline(
        pipeline_name="ingest_pipe_fallback",
        pipelines_dir="pipes",
        destination="duckdb",
        dataset_name="bronze",
        full_refresh=True,
    )

    load_info = pipeline_fallback.run(
        data=response.json()["body"],
        table_name="journeys_data",
    )

    print("*" * 50)
    print("Failed to load data into primary DuckDB-file. Loaded to fallback instead.")
    print("*" * 50)

print(pipeline.last_trace.last_extract_info)
print("-" * 10)
print(load_info)
