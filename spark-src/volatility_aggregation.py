import os
import time
from pyspark.sql import SparkSession
import pyspark.sql.functions._

# grab API key from env
try:
    DB_URL = os.environ['CJ_DB_URL']
    DB_USER = os.environ['CJ_DB_UN']
    DB_PASS = os.environ['CJ_DB_PW']
except:
    print('Missing credentials. Please set environment variables appropriately.')
    exit()

# create spark session
spark = SparkSession.builder \
    .master('local') \
    .appName('volatility_aggregation') \
    .getOrCreate()

symbolDF = spark.read \
    .format('jdbc') \
    .option('url', DB_URL) \
    .option('dbtable', 'public.symbol_master_tbl') \
    .option('user', DB_USER) \
    .option('password', DB_PASS) \
    .load()
    
# read in data
rawDF = spark.read \
    .format('jdbc') \
    .option('url', DB_URL) \
    .option('dbtable', 'public.daily_prices_temp_tbl') \
    .option('user', DB_USER) \
    .option('password', DB_PASS) \
    .load()

rawDF.createOrReplaceTempView('prices')

# function to run std dev's with map function
def aggregateDeviation(row):
    sqlDF = spark.sql('SELECT TOP 14 FROM prices WHERE symbol = ' + row.symbol)
    sqlDF.select(mean(sqlDF('close'))).show()
    return row

# run the aggregation per company in parallel
symbolDF.rdd.map(aggregateDeviation)
