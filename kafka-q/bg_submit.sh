#!/bin/bash
# submit as background task

nohup python ~/crossjoinanalytics/kafka-q/producer.py > ~/k_daily_producer_log.out 2>&1
