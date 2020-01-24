from kafka import KafkaProducer
from kafka.errors import KafkaError
import time

producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

counter = 0

while(True):
    producer.send('test', b'message '+ str(counter))
    counter = counter + 1
    time.sleep(0.5)