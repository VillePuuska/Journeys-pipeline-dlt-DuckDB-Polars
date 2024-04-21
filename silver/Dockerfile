FROM python:3.10.14-slim

WORKDIR /transformations

COPY requirements.txt requirements.txt

RUN pip install -r requirements.txt

COPY transform.py transform.py

COPY utils utils

COPY db_operations db_operations

WORKDIR /data

ENTRYPOINT [ "python", "/transformations/transform.py" ]
