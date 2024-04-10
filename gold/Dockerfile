FROM python:3.10.14-slim

WORKDIR /export

COPY requirements.txt requirements.txt

RUN pip install -r requirements.txt

COPY export.py export.py

WORKDIR /data

ENTRYPOINT [ "python", "/export/export.py" ]
