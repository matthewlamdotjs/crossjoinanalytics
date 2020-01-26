from kafka import KafkaProducer
from kafka.errors import KafkaError
import time
import requests
import psycopg2
import os


try:
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

    cursor.execute(
        'SELECT symbol FROM symbol_master_tbl;'
    )

    symbols = cursor.fetchall()

    cursor.close()
    connection.close()

    cont = True

    while(cont):

        for symbol in symbols:

            print(symbol)

            # url = ('https://www.alphavantage.co/query?'+
            #                 'function=SYMBOL_SEARCH'	+ '&' +
            #                 'keywords='+ eod_symbol		+ '&' +
            #                 'apikey=' + API_KEY)

            # response = requests.get(url = url).content

            # response = open('example.json', 'r').read()

            # producer.send('test', b''+response)

            # time.sleep(0.5)
        
        cont = False;

except (Exception, psycopg2.Error) as error :
    print ('Error while connecting to PostgreSQL', error)
