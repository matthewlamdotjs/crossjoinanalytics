import os
import time
from pyspark.sql import SparkSession
from datetime import datetime, timedelta

# grab API key from env
try:
    DB_URL = os.environ['CJ_DB_URL']
    DB_PORT = os.environ['CJ_DB_PORT']
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
    .option('url', DB_URL+':'+DB_PORT) \
    .option('dbtable', 'public.symbol_master_tbl') \
    .option('user', DB_USER) \
    .option('password', DB_PASS) \
    .option('driver', 'org.postgresql.Driver') \
    .load()
    
# read in data
rawDF = spark.read \
    .format('jdbc') \
    .option('url', DB_URL) \
    .option('dbtable', 'public.daily_prices_temp_tbl') \
    .option('user', DB_USER) \
    .option('password', DB_PASS) \
    .option('driver', 'org.postgresql.Driver') \
    .load()

# make table available from sparksql
rawDF.createOrReplaceTempView('prices')

start_date = (date.today() - timedelta(days=1)).strftime('%Y-%m-%d')
end_date = (date.today() - timedelta(days=15)).strftime('%Y-%m-%d')

sqlDF = spark.sql("""
    SELECT
        symbol,
        '"""+ start_date +"""' AS start_date,
        '"""+ end_date +"""' AS end_date,
        stddev(price_close) AS price_deviation,
        avg(price_close) AS average_price
    FROM
        daily_prices_temp_tbl
    WHERE
        CAST(date AS INT)
        BETWEEN unix_timestamp('"""+ start_date +"""', 'yyyy-MM-dd HH:mm:ss')
        AND unix_timestamp('"""+ end_date +"""', 'yyyy-MM-dd HH:mm:ss')
    GROUP BY
        symbol
    ORDER BY
        symbol;
""")
