FROM python:3.7-slim-stretch

WORKDIR /app/historical_db

COPY ./. /app/historical_db

RUN pip3 install --no-cache-dir -r ./requirements.txt