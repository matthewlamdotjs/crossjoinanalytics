from kafka import KafkaProducer
from kafka.errors import KafkaError
import time
import requests
import psycopg2
import os


try:
    API_KEY = os.environ['ALPHA_VANTAGE_API_KEY']
    DB_URL = os.environ['CJ_DB_URL']
    DB_PORT = os.environ['CJ_DB_PORT']
    DB_USER = os.environ['CJ_DB_UN']
    DB_PASS = os.environ['CJ_DB_PW']
except:
    print('Missing credentials. Please set environment variables appropriately.')
    exit()

try:
    # Connect Kafka
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

    # Connect DB
    connection = psycopg2.connect(user = DB_USER,
                                  password = DB_PASS,
                                  host = DB_URL,
                                  port = DB_PORT,
                                  database = 'postgres')
    connection.autocommit = True
    cursor = connection.cursor()

    # Get symbols
    cursor.execute(
        'SELECT symbol FROM symbol_master_tbl;'
    )

    rows = cursor.fetchall()

    cursor.close()
    connection.close()

    while(True):

        for row in rows:

            symbol = row[0]

            url = ('https://www.alphavantage.co/query?'+
                            'function=TIME_SERIES_DAILY'+ '&' +
                            'symbol='+ symbol	    	+ '&' +
                            'apikey=' + API_KEY)

            
            response = requests.get(url = url).content

            # send message payload to queue
            producer.send('stock-prices', b''+response)

            # wait for API limit
            time.sleep(0.5)

except (Exception, psycopg2.Error) as error :
    print ('Error while connecting to PostgreSQL', error)
