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

# create spark stream
sc = SparkContext('local', 'kafka-in')
ssc = StreamingContext(sc, 0.5)

# create kafka stream
directKafkaStream = KafkaUtils.createDirectStream(ssc, ['stock-prices'],
    {'bootstrap.servers': SERVERS})

# create spark session
spark = SparkSession.builder \
    .appName('stream-consumer') \
    .config('spark.jars', DRIVER_PATH) \
    .getOrCreate()

# create currency converter
c = CurrencyConverter()
spark.udf.register('convert_to_usd', lambda x,y : c.convert(x, y, 'USD'))

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

# function to apply to each streamed item
def processStream(time, rdd):

    def rddProcess(record):

        # get message value
        response = record[1]
    
        try:
            # parse json
            js_payload = json.loads(response)
            symbol = js_payload['Meta Data']['2. Symbol']
            ts = js_payload['Time Series (Daily)']

            # aggregate response into a dataframe-convertable format
            def normalize(row):
                nested = row[1]
                return [symbol, row[0], nested['2. high'], nested['3. low'], nested['1. open'], nested['4. close'], time]
            
            thelist = list(
                map(normalize, list(map(list, ts.items())))
            )
            
            print(thelist)

        except (Exception) as error :
            print('PySparkError: ' + str(error))

    rdd.foreach(rddProcess)

directKafkaStream.foreachRDD(processStream)

# start stream
ssc.start()
ssc.awaitTermination()

# end spark session
spark.stop()
