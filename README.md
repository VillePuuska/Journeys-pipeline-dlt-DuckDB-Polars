# JourneysAPI pipeline
Simple example of an ELT pipeline using dlt for ingesting Tampere public transit data from the JourneysAPI, DuckDB for intermediate storage, and DuckDB and Polars for transformations.

Each stage can also be packaged as its own Docker container with a provided Dockerfile.

Steps for running the stages as Docker containers:
- Make/choose a directory you want to store all the data in, e.g. `/data`
- Bronze:
  - Cd into `/bronze`
  - Build the container with e.g.: `docker build -t ingest:0.1 .`
  - Run the container with the data directory bind mounted to persist the results and metadata. For example, cd into the data directory and run `docker run --rm --mount type=bind,src="$(pwd)",target=/data ingest:0.1` This binds the current directory to the containers `/data` directory where the results and metadata are stored. This way you could run the `ingest.py` script both from the container and directly from your host machine; although this would likely require some chowns.
  - The container prints out some info about the ingestion. You might want to pipe this to a log file.
- Silver:
  - Cd into `/silver`
  - Build the container with e.g.: `docker build -t transform:0.1 .`
  - Run the container with the data directory bind mounted to persist the results like with the bronze container. Include an env-file to pass the environment variables needed. For example, if your data directory is under the root directory of this repo, then you can use the env-file in `/silver` by running `docker run --rm --mount type=bind,src="$(pwd)",target=/data --env-file ../silver/env transform:0.1`
  - The container prints out info about the loaded and transformed data. You might want to pipe this to a log file.
