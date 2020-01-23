import requests
import os
from os import listdir
from os.path import isfile, join
import re
import csv
import time
import json

# grab API key from env
try:
    API_KEY = os.environ['ALPHA_VANTAGE_API_KEY']
except:
    print("Missing API key. Please set environment variable as ALPHA_VANTAGE_API_KEY.")
    exit()

# get filenames
onlyfiles = [f for f in listdir('symbol_lists/') if isfile(join('symbol_lists/', f))]

output = 'symbol,name,type,region,marketOpen,marketClose,timezone,currency\n'

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
        
        # url = ('https://www.alphavantage.co/query?'+
        #     'function=TIME_SERIES_DAILY'	+ '&' +
        #     'symbol=AHT'						+ '&' +
        #     'outputsize=full'				+ '&' +
        #     'apikey=' + API_KEY)

        for row in csv_array:
            #print(output)

            eod_symbol = row[symbol_index]

            url = ('https://www.alphavantage.co/query?'+
                'function=SYMBOL_SEARCH'	+ '&' +
                'keywords='+ eod_symbol		+ '&' +
                'apikey=' + API_KEY)

            response = requests.get(url = url).content

            try:
                if len(json.loads(response)['bestMatches']) > 0:

                    for match in json.loads(response)['bestMatches']:

                        output = (output +
                            match['1. symbol'] + ',' +
                            match['2. name'] + ',' +
                            match['3. type'] + ',' +
                            match['4. region'] + ',' +
                            match['5. marketOpen'] + ',' +
                            match['6. marketClose'] + ',' +
                            match['7. timezone'] + ',' +
                            match['8. currency'] + '\n')
            except:
                print('no results for ' + eod_symbol)
                
            # 5 requests per minute api limit
            time.sleep(0.5)

# write output
f = open('output.csv', 'w')
f.write(output)
f.close()


