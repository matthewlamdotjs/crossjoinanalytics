from kafka import KafkaProducer
from kafka.errors import KafkaError
import time
import requests

producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

cont = True

while(cont):
    # url = ('https://www.alphavantage.co/query?'+
    #                 'function=SYMBOL_SEARCH'	+ '&' +
    #                 'keywords='+ eod_symbol		+ '&' +
    #                 'apikey=' + API_KEY)

    # response = requests.get(url = url).content

    response = open('example.json', 'r').read()

    producer.send('test', b''+response)
    
    cont = False;
    time.sleep(0.5)