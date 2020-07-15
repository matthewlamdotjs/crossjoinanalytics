import requests
import os
from os import listdir
from os.path import isfile, join
import re
import csv
import time
import json
import psycopg2

# grab API key and db cnx props from env
try:
    API_KEY = os.environ['ALPHA_VANTAGE_API_KEY_DEV']
    DB_URL = 'localhost'
    DB_PORT = '5432'
    DB_USER = 'postgres'
    DB_PASS = os.environ['LOCAL_DB_PW']
except:
    print('Missing credentials. Please set environment variables appropriately.')
    exit()

# get filenames for symbol lists
onlyfiles = [f for f in listdir('symbol_lists/') if isfile(join('symbol_lists/', f))]

# setup db connection
try:

    connection = psycopg2.connect(user = DB_USER,
                                  password = DB_PASS,
                                  host = DB_URL,
                                  port = DB_PORT,
                                  database = 'hedgemaster')
    connection.autocommit = True
    cursor = connection.cursor()

    # parse files
    for file_name in onlyfiles:
        with open('symbol_lists/' + file_name, 'r') as csvfile:
            
            csv_array = []
            
            csvfilereader = csv.reader(csvfile, delimiter='\t', quotechar='"')
            
            for row in csvfilereader:
                csv_array.append(row)
            
            input_headers = csv_array.pop(0)
            symbol_index = input_headers.index('Symbol')
            description_index = input_headers.index('Description')

            for row in csv_array:

                eod_symbol = row[symbol_index]

                url = ('https://www.alphavantage.co/query?'+
                    'function=SYMBOL_SEARCH'	+ '&' +
                    'keywords='+ eod_symbol		+ '&' +
                    'apikey=' + API_KEY)

                response = requests.get(url = url).content

                try:
                    if len(json.loads(response)['bestMatches']) > 0:

                        for match in json.loads(response)['bestMatches']:

                            query_string = ('CALL "addSymbol"(\''+
                                match['1. symbol'] +'\',\''+
                                match['2. name'] +'\',\''+
                                match['3. type'] +'\',\''+
                                match['4. region'] +'\',\''+
                                match['7. timezone'] +'\',\''+
                                match['8. currency'] +'\');')
                            
                            print(query_string)
                            cursor.execute(query_string)
                except:
                    print('no matches for '+eod_symbol)

                # 5 requests per minute api limit
                time.sleep(12)


except (Exception, psycopg2.Error) as error :
    print ('Error while connecting to PostgreSQL', error)
finally:
    #closing database connection.
    if(connection):
        cursor.close()
        connection.close()
        print('PostgreSQL connection is closed')
