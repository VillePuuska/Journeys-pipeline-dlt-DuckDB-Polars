#!/bin/sh

eval $(minikube docker-env)
docker build -t ingest:0.1 ./bronze
docker build -t transform:0.1 ./silver
docker build -t export:0.1 ./gold

kubectl create secret generic transform-env --from-env-file=silver/env --namespace=argo
kubectl create secret generic export-env --from-env-file=gold/env --namespace=argo

kubectl apply -n argo -f argo/persistent-volume.yaml
kubectl apply -n argo -f argo/persistent-volume-claim.yaml
