import json 

response = open('example.json', 'r').read()

js_payload = json.loads(response)

symbol = js_payload['Meta Data']['2. Symbol']
ts = js_payload['Time Series (Daily)']


ts.keys()