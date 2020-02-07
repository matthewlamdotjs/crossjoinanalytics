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
    .master('spark://localhost:7077') \
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

    # grab next off of message queue
    taken = rdd.take(1)
    for record in taken:

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

            # create dataframe from payload
            newDF = spark.createDataFrame(thelist, [
                'symbol',
                'date',
                'price_high',
                'price_low',
                'price_open',
                'price_close'
            ])

            # make table available from sparksql
            newDF.createOrReplaceTempView('new_prices')
            rawDF.createOrReplaceTempView('current_prices')

            # add currency join for conversion
            sqlDF = spark.sql("""
                SELECT
                    new_prices.symbol,
                    cast(new_prices.date as date),
                    cast(new_prices.price_high as decimal(8,4)),
                    cast(new_prices.price_low as decimal(8,4)),
                    cast(new_prices.price_open as decimal(8,4)),
                    cast(new_prices.price_close as decimal(8,4)),
                    cast(convert_to_usd(
                        new_prices.price_close, symbol_master_tbl.currency
                    )
                    as decimal(8,4)) as price_usd
                FROM
                    new_prices
                LEFT JOIN
                    symbol_master_tbl
                ON
                    new_prices.symbol = symbol_master_tbl.symbol
                WHERE
                    new_prices.date NOT IN
                    (
                        SELECT
                            current_prices.date
                        FROM
                            current_prices
                    )
            """)

            # write results to db
            sqlDF.write.mode('append') \
                .format('jdbc') \
                .option('url', 'jdbc:postgresql://'+DB_URL+':'+DB_PORT+'/postgres') \
                .option('dbtable', 'daily_prices_temp_tbl') \
                .option('user', DB_USER) \
                .option('password', DB_PASS) \
                .option('driver', 'org.postgresql.Driver') \
                .save()

        except (Exception) as error :
            print('PySparkError: ' + str(error))

directKafkaStream.foreachRDD(processStream)

# start stream
ssc.start()
ssc.awaitTermination()

# end spark session
spark.stop()
