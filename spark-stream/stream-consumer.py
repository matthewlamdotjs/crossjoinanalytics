from pyspark.streaming.kafka import KafkaUtils
import os
import json
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from currency_converter import CurrencyConverter # pip install currencyconverter
import psycopg2
import pandas.io.sql as psql
import pandas as pd 
import multiprocessing


# load env vars
try:
    DB_URL = os.environ['CJ_DB_URL']
    DB_PORT = os.environ['CJ_DB_PORT']
    DB_USER = os.environ['CJ_DB_UN']
    DB_PASS = os.environ['CJ_DB_PW']
    SERVERS = os.environ['K_SERVERS']
    ZOOKEEPER = os.environ['Z_SERVER']
    DRIVER_PATH = os.environ['PG_JDBC_DRIVER']
except:
    print('Missing credentials. Please set environment variables appropriately.')
    exit()

# create spark stream
sc = SparkContext(appName = 'pyspark-kstream-consumer')
ssc = StreamingContext(sc, 1)

directKafkaStream = KafkaUtils.createDirectStream(ssc, ['stock-prices'],
    {'bootstrap.servers': SERVERS})

# function to apply to each streamed RDD
def processStream(time, rdd):

    # RDD partition-level processing, runs in parallel so put all expensive operations at this level
    def rddProcess(partition):

        # Connect DB
        connection = psycopg2.connect(user = DB_USER,
                                        password = DB_PASS,
                                        host = DB_URL,
                                        port = DB_PORT,
                                        database = 'postgres')
        connection.autocommit = True

        cursor = connection.cursor()

        # DF to postgres function
        # pandas.io.sql patched function from source
        # https://gist.github.com/jorisvandenbossche/10841234
        def _write_postgresql(frame, table, names, cur):
            bracketed_names = ['"' + column + '"' for column in names]
            col_names = ','.join(bracketed_names)
            wildcards = ','.join([r'%s'] * len(names))
            insert_query = 'INSERT INTO public.%s (%s) VALUES (%s)' % (
                table, col_names, wildcards)
            data = [tuple(x) for x in frame.values]
            print insert_query
            print data
            cur.executemany(insert_query, data)

        # create individual message process function
        def processMessage(record):
            # get message value
            response = record[1]

            symbol = 'error'

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

                # Get symbol currency
                cursor.execute(
                    'SELECT currency FROM symbol_master_tbl where symbol = \''+symbol+'\';'
                )

                currency = cursor.fetchone()[0]

                # create currency converter
                c = CurrencyConverter() # c.convert(x, y, 'USD')
                def convert_usd(price_close):
                    return c.convert(price_close, currency, 'USD')

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

                # subtract old data
                key_diff = set(newDF.date).difference(datesDF.date)
                where_diff = newDF.date.isin(key_diff)

                to_insert = newDF[where_diff]
                to_insert['price_usd'] = to_insert['price_close'].apply(convert_usd)

                print(currency)
                print(to_insert.head())

                _write_postgresql(
                    to_insert,
                    'daily_prices_temp_tbl',
                    [s.replace(' ', '_').strip() for s in to_insert.columns],
                    cursor
                )

            except (Exception) as error :
                print('PG Error: ' + str(error))
                return (symbol, str(error))

            return (symbol, 1)

        return map(processMessage, partition)

    print(rdd.mapPartitions(rddProcess).collect())

directKafkaStream.foreachRDD(processStream)

# start stream
ssc.start()
ssc.awaitTermination()

# end spark session
spark.stop()
