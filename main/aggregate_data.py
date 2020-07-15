import os
import time
from datetime import date, datetime, timedelta
import psycopg2

# load env vars
try:
    DB_URL = 'localhost'
    DB_PORT = '5432'
    DB_USER = 'postgres'
    DB_PASS = os.environ['LOCAL_DB_PW']
except:
    print('Missing credentials. Please set environment variables appropriately.')
    exit()


# Connect DB
connection = psycopg2.connect(user = DB_USER,
                                password = DB_PASS,
                                host = DB_URL,
                                port = DB_PORT,
                                database = 'hedgemaster')
connection.autocommit = True
cursor = connection.cursor()

# Check if initialized or not
cursor.execute('SELECT COUNT(1) FROM volatility_aggregation_tbl')
count = cursor.fetchone()[0]

# Find start date
if(count > 0):
    cursor.execute(
        'SELECT start_date + interval \'7 days\' FROM volatility_aggregation_tbl ORDER BY start_date DESC LIMIT 1;'
    )
else:
    cursor.execute(
        'SELECT date FROM daily_prices_temp_tbl ORDER BY date ASC LIMIT 1;'
    )
start = cursor.fetchone()[0]
end = start + timedelta(days=14)

while(end < date.today()):

    # define moving window frame
    start_date = (start).strftime('%Y-%m-%d')
    end_date = (end).strftime('%Y-%m-%d')

    # aggregate data using std dev and mean for each window
    sql_agg = """
        INSERT INTO volatility_aggregation_tbl
        SELECT * FROM (
            SELECT
                symbol,
                cast('"""+ start_date +"""' as date) AS start_date,
                cast('"""+ end_date +"""' as date) AS end_date,
                cast(stddev(price_usd) as decimal(8,4)) AS price_deviation,
                cast(avg(price_usd) as decimal(8,4)) AS average_price
            FROM
                daily_prices_temp_tbl
            WHERE
                date >= cast('"""+ start_date +"""' as date) AND
                date <= cast('"""+ end_date +"""' as date)
            GROUP BY
                symbol
            ORDER BY
                symbol
        ) AS temp_agg_tbl
        WHERE price_deviation IS NOT NULL;
    """

    # perform the insert
    cursor.execute(sql_agg)

    # skip a week
    start = start + timedelta(days=7)
    end = end + timedelta(days=7)

