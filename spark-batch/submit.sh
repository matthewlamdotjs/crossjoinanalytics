#!/bin/bash

/usr/local/spark/bin/spark-submit \
  --driver-class-path /usr/local/postgresql-42.2.9.jar \
  --master spark://ec2-44-232-120-145.us-west-2.compute.amazonaws.com:7077 \
  --conf "spark.cores.max=12" --conf "spark.executor.cores=4" \
  --driver-memory 4g \
  --executor-memory 2g \
  --executor-cores 4 \
  ~/crossjoinanalytics/spark-batch/volatility_aggregation.py \
  1000
  