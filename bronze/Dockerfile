FROM python:3.10.14-alpine3.19

WORKDIR /pipeline

COPY requirements.txt requirements.txt

RUN pip install -r requirements.txt

COPY ingest.py ingest.py

WORKDIR /data

ENTRYPOINT [ "python", "/pipeline/ingest.py" ]
