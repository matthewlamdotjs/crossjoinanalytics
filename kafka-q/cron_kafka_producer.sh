#!/bin/bash
# send job every day at midnight

0 0 * * * ~/crossjoinanalytics/kafka-q/bg_submit.sh myjob.py
