#!/bin/bash

/usr/local/spark/bin/spark-submit \
  --driver-class-path /usr/local/postgresql-42.2.9.jar \
  --jars ~/spark-streaming-kafka-0-8-assembly_2.11-2.4.0.jar \
  --master spark://ec2-44-232-120-145.us-west-2.compute.amazonaws.com:7077 \
  --num-executors 3 \
  --driver-memory 4g \
  --executor-memory 2g \
  --executor-cores 4 \
  ~/crossjoinanalytics/spark-stream/stream-consumer.py \
  1000
  