import os
import time
from pyspark.sql import SparkSession
from datetime import date, datetime, timedelta

# load env vars
try:
    DB_URL = os.environ['CJ_DB_URL']
    DB_PORT = os.environ['CJ_DB_PORT']
    DB_USER = os.environ['CJ_DB_UN']
    DB_PASS = os.environ['CJ_DB_PW']
    DRIVER_PATH = os.environ['PG_JDBC_DRIVER']
except:
    print('Missing credentials. Please set environment variables appropriately.')
    exit()

# create spark session
spark = SparkSession.builder \
    .master('local') \
    .appName('volatility_aggregation') \
    .config('spark.jars', DRIVER_PATH) \
    .getOrCreate()
    
# read in data
rawDF = spark.read \
    .format('jdbc') \
    .option('url', 'jdbc:postgresql://'+DB_URL+':'+DB_PORT+'/postgres') \
    .option('dbtable', 'daily_prices_temp_tbl') \
    .option('user', DB_USER) \
    .option('password', DB_PASS) \
    .option('driver', 'org.postgresql.Driver') \
    .load()

# make table available from sparksql
rawDF.createOrReplaceTempView('prices')

# Start window today and move back 20 years * 365 days = 7300 days
# Start 3 days ago to ensure end date is a saturday when running today (tuesday)
counter = 4
current_date = date.today()
while(counter < 7300):

    # define moving window frame
    start_date = (current_date - timedelta(days=counter + 13)).strftime('%Y-%m-%d')
    end_date = (current_date - timedelta(days=counter)).strftime('%Y-%m-%d')

    # aggregate data using std dev and mean for each window
    sqlDF = spark.sql("""
        SELECT
            symbol,
            cast('"""+ start_date +"""' as date) AS start_date,
            cast('"""+ end_date +"""' as date) AS end_date,
            cast(stddev(price_usd) as decimal(8,4)) AS price_deviation,
            cast(avg(price_usd) as decimal(8,4)) AS average_price
        FROM
            prices
        WHERE
            date >= cast('"""+ start_date +"""' as date) AND
            date <= cast('"""+ end_date +"""' as date)
        GROUP BY
            symbol
        ORDER BY
            symbol
    """).filter('price_deviation IS NOT NULL')

    # write results to db
    sqlDF.write.mode('append') \
        .format('jdbc') \
        .option('url', 'jdbc:postgresql://'+DB_URL+':'+DB_PORT+'/postgres') \
        .option('dbtable', 'volatility_aggregation_tbl') \
        .option('user', DB_USER) \
        .option('password', DB_PASS) \
        .option('driver', 'org.postgresql.Driver') \
        .save()

    counter = counter + 7

# end session
spark.stop()
