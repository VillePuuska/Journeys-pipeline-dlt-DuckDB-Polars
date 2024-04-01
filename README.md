# JourneysAPI pipeline
Simple example of an ELT pipeline using dlt for ingesting Tampere public transit data from the JourneysAPI, DuckDB for intermediate storage, and DuckDB and Polars for transformations.

Each stage can also be packaged as its own Docker container with a provided Dockerfile.

Steps for running the stages as Docker containers:
- Bronze:
  - Cd into `/bronze`
  - Build the container with e.g.: `docker build -t ingest:0.1 .`
  - Run the container with a directory bind mounted to persist the results and metadata with e.g.: `docker run --rm --mount type=bind,src="$(pwd)",target=/data ingest:0.1` This binds the current directory, i.e. `/bronze`, to the containers `/data` directory where the results and metadata are stored. This way you could run the `ingest.py` script both from the container and directly from your host machine; although this would likely require some chowns.
  - The container prints out some info about the ingestion. You might want to pipe this to a log file.
