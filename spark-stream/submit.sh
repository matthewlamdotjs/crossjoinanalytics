#!/bin/bash

/usr/local/spark/bin/spark-submit \
  --driver-class-path /usr/local/postgresql-42.2.9.jar,~/spark-streaming-kafka-0-8-assembly_2.11-2.4.0.jar \
  --master spark://localhost:7077 \
  ~/crossjoinanalytics/spark-stream/stream-consumer.py \
  1000
  