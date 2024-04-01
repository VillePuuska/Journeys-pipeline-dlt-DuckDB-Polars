import dlt
from dlt.sources.helpers import requests

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

load_info = pipeline.run(
    data=response.json()["body"],
    table_name="journeys_data",
)

print(pipeline.last_trace.last_extract_info)
print("-" * 10)
print(load_info)
