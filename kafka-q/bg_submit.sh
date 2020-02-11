#!/bin/bash
# submit as background task

source ~/.bash_profile

nohup python ~/crossjoinanalytics/kafka-q/producer.py > ~/k_daily_producer_log.out 2>&1
