#!/bin/bash

source ~/.bash_profile

/usr/local/spark/bin/spark-submit \
  --driver-class-path /usr/local/postgresql-42.2.9.jar \
  --master spark://ec2-44-232-120-145.us-west-2.compute.amazonaws.com:7077 \
  --conf "spark.cores.max=9" --conf "spark.executor.cores=3" \
  --driver-memory 6g \
  --executor-memory 3g \
  --executor-cores 3 \
  ~/crossjoinanalytics/spark-batch/volatility_aggregation.py \
  1000
  