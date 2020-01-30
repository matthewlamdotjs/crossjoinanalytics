from pyspark.streaming.kafka import KafkaUtils
import os
import time
from pyspark.sql import SparkSession
from datetime import date, datetime, timedelta
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# grab API key from env
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
# spark = SparkSession.builder \
#     .master('local') \
#     .appName('stream-consumer') \
#     .config('spark.jars', DRIVER_PATH) \
#     .getOrCreate()

directKafkaStream.foreachRDD(lambda x: x.foreach(lambda y: print(y)))
ssc.start()
ssc.awaitTermination() 

# consume messages from kafka
# for message in directKafkaStream:

#     response = message.value.decode('utf-8')
    
#     try:
#         js_payload = json.loads(response)
#         symbol = js_payload['Meta Data']['2. Symbol']
#         ts = js_payload['Time Series (Daily)']

#         # aggregates response into a dataframe-convertable format
#         def normalize(row):
#             nested = row[1]
#             return [symbol, row[0], nested['2. high'], nested['3. low'], nested['1. open'], nested['4. close'], datetime.today()]

#         thelist = list(
#             map(normalize, list(map(list, ts.items())))
#         )

#         # read in existing data for symbol
#         rawDF = spark.read \
#             .format('jdbc') \
#             .option('url', 'jdbc:postgresql://'+DB_URL+':'+DB_PORT+'/postgres') \
#             .option('dbtable', 'daily_prices_temp_tbl') \
#             .option('user', DB_USER) \
#             .option('password', DB_PASS) \
#             .option('driver', 'org.postgresql.Driver') \
#             .load() \
#             .filter('symbol = \'' + symbol + '\'')

#         # create dataframe from payload
#         newDF = spark.createDataFrame(thelist, [
#             'symbol',
#             'date',
#             'price_high',
#             'price_low',
#             'price_open',
#             'price_close',
#             'timestamp'
#         ])

#         # make tables available from sparksql
#         rawDF.createOrReplaceTempView('current_prices')
#         newDF.createOrReplaceTempView('new_prices')

#         # left anti join on composite key to remove duplicates
#         sqlDF = spark.sql("""
#             SELECT
#                 symbol,
#                 date,
#                 price_high,
#                 price_low,
#                 price_open,
#                 price_close,
#                 timestamp
#             FROM
#                 new_prices
#             WHERE
#                 new_prices.date NOT IN
#                 (
#                     SELECT
#                         current_prices.date
#                     FROM
#                         current_prices
#                 )
#         """)

#         # write results to db
#         sqlDF.write.mode('append') \
#             .format('jdbc') \
#             .option('url', 'jdbc:postgresql://'+DB_URL+':'+DB_PORT+'/postgres') \
#             .option('dbtable', 'daily_prices_temp_tbl') \
#             .option('user', DB_USER) \
#             .option('password', DB_PASS) \
#             .option('driver', 'org.postgresql.Driver') \
#             .save()

#     except (Exception) as error :
#         print(error)

# # end session
# spark.stop()
