# import json 

# response = open('example.json', 'r').read()

# js_payload = json.loads(response)

# symbol = js_payload['Meta Data']['2. Symbol']
# ts = js_payload['Time Series (Daily)']

# #for key in ts.keys():

print ('CALL "consumeRawData"(\''+
                    '3882.HK' +'\',\''+
                    '2020-01-23' +'\',\''+
                    '0.3000' +'\',\''+
                    '0.3000' +'\',\''+
                    '0.3000' +'\',\''+
                    '0.3000' +'\');')
