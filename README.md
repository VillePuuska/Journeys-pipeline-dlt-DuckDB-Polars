# JourneysAPI pipeline
Simple example of an ELT pipeline using dlt for ingesting Tampere public transit data from the JourneysAPI, DuckDB for intermediate storage, and DuckDB and Polars for transformations. Done as an exercise to see how the technologies might fit together and to find possible pitfalls and shortcomings of the stack.

Each stage can be run as a standalone script or packaged as its own Docker container with a provided Dockerfile. Orchestration the entire pipeline of containers done with Argo Workflows.

![Diagram of the pipeline](./journeys-pipeline-diagram.png)

---

Steps for building and running the stages as Docker containers:
- Make/choose a directory you want to store all the data in, e.g. `/data`. _Run all the containers from this directory._
- Bronze:
  - Build the container image in `/bronze` with e.g.: `docker build -t ingest:0.1 .`
  - Run the container with the data directory bind mounted to persist the results and metadata. For example, cd into the data directory and run `docker run --rm --mount type=bind,src="$(pwd)",target=/data ingest:0.1` This binds the current directory to the containers `/data` directory where the results and metadata are stored.
  - The container prints out some info about the ingestion. You might want to pipe this to a log file.
- Silver:
  - Build the container image in `/silver` with e.g.: `docker build -t transform:0.1 .`
  - Run the container with the data directory bind mounted to persist the results like with the bronze container. Include an env-file to pass the environment variables needed. For example, if your data directory is directly under the root directory of this repo, then you can use the env-file in `/silver` by running `docker run --rm --mount type=bind,src="$(pwd)",target=/data --env-file ../silver/env transform:0.1`
  - The container prints out info about the loaded and transformed data. You might want to pipe this to a log file.
- Gold:
  - Build the container image in `/gold` with e.g.: `docker build -t export:0.1 .`
  - Run the container with the data directory bind mounted again. Include an env-file to pass the environment variables needed. For example, if your data directory is directly under the root directory of this repo, then you can use the env-file in `/gold` by running `docker run --rm --mount type=bind,src="$(pwd)",target=/data --env-file ../gold/env export:0.1`
  - The container prints out info about the data exported from the silver DuckDB database to the final Delta table. You might want to pipe this to a log file.
- Cleaning up the final Delta table:
  - TODO (`clean_delta_table/optimize_and_vacuum.py`)

---

Steps for getting Argo running in Minikube locally and running the pipeline:
- install Minikube:
```
curl -LO https://storage.googleapis.com/minikube/releases/latest/minikube-linux-amd64
sudo install minikube-linux-amd64 /usr/local/bin/minikube && rm minikube-linux-amd64
```
- start Minikube:
```
minikube start
```
- install Argo CLI (not needed if using Argo UI):
```
curl -sLO https://github.com/argoproj/argo-workflows/releases/download/v3.5.5/argo-linux-amd64.gz
gunzip argo-linux-amd64.gz
chmod +x argo-linux-amd64
sudo mv ./argo-linux-amd64 /usr/local/bin/argo
```
- install and start Argo:
```
kubectl create namespace argo
kubectl apply -n argo -f https://github.com/argoproj/argo-workflows/releases/download/v3.5.5/quick-start-minimal.yaml
```
- if you want to use the Argo UI, forward the port:
```
kubectl -n argo port-forward service/argo-server 2746:2746
```
- to build the images in a way that k8s can use them locally, first run
```
eval $(minikube docker-env)
```
- after the above command, build the images in the same shell session
- cd to the `/data` directory:
```
cd /workspaces/Journeys-pipeline-dlt-DuckDB-Polars/data
```
- create secrets in k8s from the env files in `/silver` and `/gold`:
```
kubectl create secret generic transform-env --from-env-file=../silver/env --namespace=argo
kubectl create secret generic export-env --from-env-file=../gold/env --namespace=argo
```
- create persistent volume and claim:
```
kubectl apply -n argo -f ../argo/persistent-volume.yaml
kubectl apply -n argo -f ../argo/persistent-volume-claim.yaml
```
- run and monitor the workflow for the pipeline assuming the workflow yaml-file is in `../argo/elt-workflow.yaml`:
```
argo submit -n argo --watch ../argo/elt-workflow.yaml
```
