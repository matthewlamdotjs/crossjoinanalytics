import json
from datetime import date, datetime, timedelta

response = open('example.json', 'r').read()

js_payload = json.loads(response)
symbol = js_payload['Meta Data']['2. Symbol']
ts = js_payload['Time Series (Daily)']

# aggregates response into a dataframe-convertable format
def normalize(row):
    nested = row[1]
    return [symbol, row[0], nested['2. high'], nested['3. low'], nested['1. open'], nested['4. close'], datetime.today()]

thelist = list(
    map(normalize, list(map(list, ts.items())))
)

print thelist[0]
