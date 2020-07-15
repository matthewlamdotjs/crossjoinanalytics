import multiprocessing
import time
import requests
import psycopg2
import os
import json
import pandas.io.sql as psql
import pandas as pd
from currency_converter import CurrencyConverter # pip install currencyconverter

# load env vars
try:
    API_KEY = os.environ['ALPHA_VANTAGE_API_KEY_DEV']
    DB_URL = 'localhost'
    DB_PORT = '5432'
    DB_USER = 'postgres'
    DB_PASS = os.environ['LOCAL_DB_PW']
except:
    print('Missing credentials. Please set environment variables appropriately.')
    exit()

try:
    # Connect DB
    connection = psycopg2.connect(user = DB_USER,
                                  password = DB_PASS,
                                  host = DB_URL,
                                  port = DB_PORT,
                                  database = 'hedgemaster')
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

    # Get symbols
    cursor.execute(
        'SELECT symbol FROM symbol_master_tbl;'
    )

    rows = cursor.fetchall()

    # threadpool of size # of cpu cores
    # pool = multiprocessing.Pool(multiprocessing.cpu_count() - 1)
    
    # callback for threads
    def send_message(r):
        try:
            response = r.content

            # write to db
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
        except:
            pass

    # add symbol queries to threadpool
    for row in rows:

        symbol = row[0]

        url = ('https://www.alphavantage.co/query?'+
                        'function=TIME_SERIES_DAILY'+ '&' +
                        'symbol='+ symbol	    	+ '&' +
                        'outputsize=full&' +
                        'apikey=' + API_KEY)

        # throw into threadpool
        # pool.apply_async(requests.get, args=[url], callback=send_message)
        send_message(requests.get(url))

        # wait for API limit
        time.sleep(12)

    
    cursor.close()
    connection.close()

except (Exception, psycopg2.Error) as error :
    print ('Error while connecting to PostgreSQL', error)
