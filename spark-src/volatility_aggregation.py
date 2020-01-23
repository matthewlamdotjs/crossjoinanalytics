import os
import time
import spark

# grab API key from env
try:
    API_KEY = os.environ['ALPHA_VANTAGE_API_KEY']
except:
    print("Missing credentials. Please set environment variables appropriately.")
    exit()
    


