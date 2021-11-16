# Kafka AVRO Python

This repo shows the avro capabilities of kafka in Python.

## Running
```
docker compose up -d
```
Wait for Kafka to start (5-10 sec)

Open two CLIs:
CLI1 (this registers the schema on first produce, and then produces the other messages):
```bash
python produce.py
```
CLI2 (this consumes the topic and prints the messages):
```bash
python consume.py
```