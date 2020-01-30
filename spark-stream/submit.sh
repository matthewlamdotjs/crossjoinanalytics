#!/bin/bash

/usr/local/spark/bin/spark-submit \
  --packages org.apache.spark:spark-streaming-kafka-0-8:2.4.0 \
  --driver-class-path /usr/local/postgresql-42.2.9.jar \
  --master spark://localhost:7077 \
  ~/crossjoinanalytics/spark-stream/stream-consumer.py \
  1000
  