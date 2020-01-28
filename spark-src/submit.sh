#!/bin/bash

/usr/local/spark/bin/spark-submit \
  --driver-class-path /usr/local/postgresql-42.2.9.jar \
  --master spark://localhost:7077 \
  ~/crossjoinanalytics/spark-src/volatility_aggregation.py \
  1000