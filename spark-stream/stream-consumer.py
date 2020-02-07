from pyspark.streaming.kafka import KafkaUtils
import os
import json
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from currency_converter import CurrencyConverter # pip install currencyconverter
import psycopg2
import pandas.io.sql as psql
import pandas as pd 


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
                return [symbol, row[0], nested['2. high'], nested['3. low'], nested['1. open'], nested['4. close']]
            
            thelist = list(
                map(normalize, list(map(list, ts.items())))
            )

            # create currency converter
            c = CurrencyConverter() # c.convert(x, y, 'USD')

            # Connect DB
            connection = psycopg2.connect(user = DB_USER,
                                            password = DB_PASS,
                                            host = DB_URL,
                                            port = DB_PORT,
                                            database = 'postgres')
            connection.autocommit = True

            cursor = connection.cursor()

            # Get symbol currency
            cursor.execute(
                'SELECT currency FROM symbol_master_tbl where symbol = \''+symbol+'\';'
            )
            currency = cursor.fetchone()[0]

            print('currency: '+ currency)
            
            # get exisiting data
            datesDF = psql.read_sql("""
                SELECT
                    date
                FROM
                    daily_prices_temp_tbl
                WHERE
                    symbol = '"""+symbol+"""';
            """, connection)

            # make DF from new data
            newDF = pd.DataFrame(thelist, columns =['symbol','date','price_high','price_low','price_open','price_close'])

            print(newDF.head())

            # subtract old data
            key_diff = set(newDF.date).difference(datesDF.date)
            where_diff = newDF.date.isin(key_diff)

            print(newDF[where_diff].head())

        except (Exception) as error :
            print('PySparkError: ' + str(error))

    rdd.foreach(rddProcess)

directKafkaStream.foreachRDD(processStream)

# start stream
ssc.start()
ssc.awaitTermination()

# end spark session
spark.stop()
