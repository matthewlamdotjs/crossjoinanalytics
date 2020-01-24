from kafka import KafkaProducer
from kafka.errors import KafkaError

producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

counter = 0

while(True):
    producer.send('test', b'message '+ counter)
    counter++