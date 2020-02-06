from pyspark.streaming.kafka import KafkaUtils
import os
import time
import json
from pyspark.sql import SparkSession
from datetime import date, datetime, timedelta
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from currency_converter import CurrencyConverter # pip install currencyconverter
import pyspark.sql.functions as F
from pyspark.sql.types import FloatType


# load env vars
try:
    DB_URL = os.environ['CJ_DB_URL']
    DB_PORT = os.environ['CJ_DB_PORT']
    DB_USER = os.environ['CJ_DB_UN']
    DB_PASS = os.environ['CJ_DB_PW']
    SERVERS = os.environ['K_SERVERS']
    DRIVER_PATH = os.environ['PG_JDBC_DRIVER']
except:
    print('Missing credentials. Please set environment variables appropriately.')
    exit()

# create spark session
spark = SparkSession.builder \
    .master('local') \
    .appName('stream-consumer') \
    .config('spark.jars', DRIVER_PATH) \
    .getOrCreate()

# create currency converter with
# https://stackoverflow.com/questions/52659955/pyspark-currency-converter
c = CurrencyConverter()
convert_curr = F.udf(lambda x,y : c.convert(x, y, 'USD'), FloatType())

# read in existing data for symbol
symbolDF = spark.read \
    .format('jdbc') \
    .option('url', 'jdbc:postgresql://'+DB_URL+':'+DB_PORT+'/postgres') \
    .option('dbtable', 'symbol_master_tbl') \
    .option('user', DB_USER) \
    .option('password', DB_PASS) \
    .option('driver', 'org.postgresql.Driver') \
    .load()

# make table available from sparksql
symbolDF.createOrReplaceTempView('symbol_master_tbl')

# read in existing data for symbol
rawDF = spark.read \
    .format('jdbc') \
    .option('url', 'jdbc:postgresql://'+DB_URL+':'+DB_PORT+'/postgres') \
    .option('dbtable', 'daily_prices_temp_tbl') \
    .option('user', DB_USER) \
    .option('password', DB_PASS) \
    .option('driver', 'org.postgresql.Driver') \
    .load() \
    .filter('symbol = \'' + symbol + '\'')

# make table available from sparksql
rawDF.createOrReplaceTempView('new_prices')

# add currency join for conversion
newDF = spark.sql("""
    SELECT
        new_prices.symbol,
        symbol_master_tbl.currency,
        cast(new_prices.date as date),
        cast(new_prices.price_high as decimal(8,4)),
        cast(new_prices.price_low as decimal(8,4)),
        cast(new_prices.price_open as decimal(8,4)),
        cast(new_prices.price_close as decimal(8,4)),
        cast(new_prices.timestamp as timestamp)
    FROM
        new_prices
    LEFT JOIN
        symbol_master_tbl
    ON
        new_prices.symbol = symbol_master_tbl.symbol
""").withColumn('price_usd', convert_curr('price_close', 'currency'))

# remake view
newDF.createOrReplaceTempView('new_prices')

# left anti join on composite key to remove duplicates
sqlDF = spark.sql("""
    SELECT
        symbol,
        cast(date as date),
        cast(price_high as decimal(8,4)),
        cast(price_low as decimal(8,4)),
        cast(price_open as decimal(8,4)),
        cast(price_close as decimal(8,4)),
        cast(price_usd as decimal(8,4)),
        cast(timestamp as timestamp)
    FROM
        new_prices
""")

print('Inserting new rows: ')
sqlDF.show()

# # write results to db
sqlDF.write.mode('overwrite') \
    .format('jdbc') \
    .option('url', 'jdbc:postgresql://'+DB_URL+':'+DB_PORT+'/postgres') \
    .option('dbtable', 'daily_prices_temp_tbl') \
    .option('user', DB_USER) \
    .option('password', DB_PASS) \
    .option('driver', 'org.postgresql.Driver') \
    .save()

# end spark session
spark.stop()
