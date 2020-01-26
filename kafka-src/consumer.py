from kafka import KafkaConsumer
import json
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

# To consume latest messages and auto-commit offsets
consumer = KafkaConsumer('test',
                         bootstrap_servers=['localhost:9092'])

try:

    connection = psycopg2.connect(user = DB_USER,
                                  password = DB_PASS,
                                  host = DB_URL,
                                  port = DB_PORT,
                                  database = 'postgres')
    connection.autocommit = True
    cursor = connection.cursor()

    for message in consumer:
        response = message.value.decode('utf-8')

        try:
            js_payload = json.loads(response)

            symbol = js_payload['Meta Data']['2. Symbol']
            ts = js_payload['Time Series (Daily)']

            for key in ts.keys():
                
                ppd = ts[key]

                query_string = ('CALL "consumeRawData"(\''+
                    symbol +'\',\''+
                    key +'\',\''+
                    ppd['2. high'] +'\',\''+
                    ppd['3. low'] +'\',\''+
                    ppd['1. open'] +'\',\''+
                    ppd['4. close'] +'\');')

                # print(query_string)    
                cursor.callproc('consumeRawData',[symbol, key, ppd['2. high'], ppd['3. low'], ppd['1. open'], ppd['4. close']])
        except:
            print('Invalid JSON')


except (Exception, psycopg2.Error) as error :
    print ('Error while connecting to PostgreSQL', error)
finally:
    #closing database connection.
    if(connection):
        cursor.close()
        connection.close()
        print('PostgreSQL connection is closed')
    