# JourneysAPI pipeline
Simple example of an ELT pipeline using dlt for ingesting Tampere public transit data from the JourneysAPI, DuckDB for intermediate storage, and DuckDB and Polars for transformations. Done as an exercise to see how the technologies might fit together and to find possible pitfalls and shortcomings of the stack.

Each stage can be run as a standalone script or packaged as its own Docker container with a provided Dockerfile. Orchestration the entire pipeline of containers done with Argo Workflows.

![Diagram of the pipeline](./journeys-pipeline-diagram.png)

---

Steps for building and running the stages as Docker containers:
- Make/choose a directory you want to store all the data in, e.g. `/data`
- Bronze:
  - Cd into `/bronze`
  - Build the container with e.g.: `docker build -t ingest:0.1 .`
  - Run the container with the data directory bind mounted to persist the results and metadata. For example, cd into the data directory and run `docker run --rm --mount type=bind,src="$(pwd)",target=/data ingest:0.1` This binds the current directory to the containers `/data` directory where the results and metadata are stored.
  - The container prints out some info about the ingestion. You might want to pipe this to a log file.
- Silver:
  - Cd into `/silver`
  - Build the container with e.g.: `docker build -t transform:0.1 .`
  - Run the container with the data directory bind mounted to persist the results like with the bronze container. Include an env-file to pass the environment variables needed. For example, if your data directory is under the root directory of this repo, then you can use the env-file in `/silver` by running `docker run --rm --mount type=bind,src="$(pwd)",target=/data --env-file ../silver/env transform:0.1`
  - The container prints out info about the loaded and transformed data. You might want to pipe this to a log file.
- Gold:
  - TODO

---

Steps for getting Argo running locally and running the pipeline:
- TODO
