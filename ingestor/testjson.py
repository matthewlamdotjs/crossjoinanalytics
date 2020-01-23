import json

payload = ('{'+
'    \"bestMatches\": ['+
'        {'+
'            \"1. symbol\": \"ACRX\",'+
'            \"2. name\": \"AcelRx Pharmaceuticals Inc.\",'+
'            \"3. type\": \"Equity\",'+
'            \"4. region\": \"United States\",'+
'            \"5. marketOpen\": \"09:30\",'+
'            \"6. marketClose\": \"16:00\",'+
'            \"7. timezone\": \"UTC-05\",'+
'            \"8. currency\": \"USD\",'+
'            \"9. matchScore\": \"0.8571\"'+
'        },'+
'        {'+
'            \"1. symbol\": \"ACER\",'+
'            \"2. name\": \"Acer Therapeutics Inc.\",'+
'            \"3. type\": \"Equity\",'+
'            \"4. region\": \"United States\",'+
'            \"5. marketOpen\": \"09:30\",'+
'            \"6. marketClose\": \"16:00\",'+
'            \"7. timezone\": \"UTC-05\",'+
'            \"8. currency\": \"USD\",'+
'            \"9. matchScore\": \"0.8571\"'+
'        },'+
'        {'+
'            \"1. symbol\": \"ACES\",'+
'            \"2. name\": \"ALPS Clean Energy ETF\",'+
'            \"3. type\": \"ETF\",'+
'            \"4. region\": \"United States\",'+
'            \"5. marketOpen\": \"09:30\",'+
'            \"6. marketClose\": \"16:00\",'+
'            \"7. timezone\": \"UTC-05\",'+
'            \"8. currency\": \"USD\",'+
'            \"9. matchScore\": \"0.8571\"'+
'        },'+
'        {'+
'            \"1. symbol\": \"ACEIX\",'+
'            \"2. name\": \"Invesco Equity and Income Fund Class A\",'+
'            \"3. type\": \"Mutual Fund\",'+
'            \"4. region\": \"United States\",'+
'            \"5. marketOpen\": \"09:30\",'+
'            \"6. marketClose\": \"16:00\",'+
'            \"7. timezone\": \"UTC-05\",'+
'            \"8. currency\": \"USD\",'+
'            \"9. matchScore\": \"0.8571\"'+
'        },'+
'        {'+
'            \"1. symbol\": \"ACEL\",'+
'            \"2. name\": \"Accel Entertainment Inc.\",'+
'            \"3. type\": \"Equity\",'+
'            \"4. region\": \"United States\",'+
'            \"5. marketOpen\": \"09:30\",'+
'            \"6. marketClose\": \"16:00\",'+
'            \"7. timezone\": \"UTC-05\",'+
'            \"8. currency\": \"USD\",'+
'            \"9. matchScore\": \"0.7500\"'+
'        },'+
'        {'+
'            \"1. symbol\": \"ACERX\",'+
'            \"2. name\": \"Invesco Equity and Income Fund Class C\",'+
'            \"3. type\": \"Mutual Fund\",'+
'            \"4. region\": \"United States\",'+
'            \"5. marketOpen\": \"09:30\",'+
'            \"6. marketClose\": \"16:00\",'+
'            \"7. timezone\": \"UTC-05\",'+
'            \"8. currency\": \"USD\",'+
'            \"9. matchScore\": \"0.7500\"'+
'        },'+
'        {'+
'            \"1. symbol\": \"ACETX\",'+
'            \"2. name\": \"Invesco Equity and Income Fund Class Y\",'+
'            \"3. type\": \"Mutual Fund\",'+
'            \"4. region\": \"United States\",'+
'            \"5. marketOpen\": \"09:30\",'+
'            \"6. marketClose\": \"16:00\",'+
'            \"7. timezone\": \"UTC-05\",'+
'            \"8. currency\": \"USD\",'+
'            \"9. matchScore\": \"0.7500\"'+
'        },'+
'        {'+
'            \"1. symbol\": \"ANIOY\",'+
'            \"2. name\": \"Acerinox, S.A.\",'+
'            \"3. type\": \"Equity\",'+
'            \"4. region\": \"United States\",'+
'            \"5. marketOpen\": \"09:30\",'+
'            \"6. marketClose\": \"16:00\",'+
'            \"7. timezone\": \"UTC-05\",'+
'            \"8. currency\": \"USD\",'+
'            \"9. matchScore\": \"0.5714\"'+
'        },'+
'        {'+
'            \"1. symbol\": \"QNIIF\",'+
'            \"2. name\": \"Seven Aces Limited\",'+
'            \"3. type\": \"Equity\",'+
'            \"4. region\": \"United States\",'+
'            \"5. marketOpen\": \"09:30\",'+
'            \"6. marketClose\": \"16:00\",'+
'            \"7. timezone\": \"UTC-05\",'+
'            \"8. currency\": \"USD\",'+
'            \"9. matchScore\": \"0.3529\"'+
'        },'+
'        {'+
'            \"1. symbol\": \"ACEZ\",'+
'            \"2. name\": \"Ariel Clean Energy Inc.\",'+
'            \"3. type\": \"Equity\",'+
'            \"4. region\": \"United States\",'+
'            \"5. marketOpen\": \"09:30\",'+
'            \"6. marketClose\": \"16:00\",'+
'            \"7. timezone\": \"UTC-05\",'+
'            \"8. currency\": \"USD\",'+
'            \"9. matchScore\": \"0.2857\"'+
'        }'+
'    ]'+
'}')

print(len(json.loads(payload)['bestMatches']))