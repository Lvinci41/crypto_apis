import requests
import time
import hmac
import hashlib
import base64
import json
import datetime as dt 
from datetime import datetime, timedelta, timezone
import pandas as pd
import csv
import random

pk = open("key_bg.txt","r")
keys = pk.readlines()
API_KEY = keys[0].strip()
SECRET_KEY = keys[1].strip()
PASSPHRASE = keys[2].strip()
BASE_URL = "https://api.bitget.com"
ENDPOINT = "/api/v2/spot/market/history-candles"
URL = BASE_URL + ENDPOINT

def get_timestamp():
  return int(time.time() * 1000)

def datetime_format(timestamp):
    return dt.datetime.fromtimestamp(int(timestamp)/1000.0)    

def sign(message, secret_key):
  mac = hmac.new(bytes(secret_key, encoding='utf8'), bytes(message, encoding='utf-8'), digestmod='sha256')
  d = mac.digest()
  return base64.b64encode(d)

def pre_hash(timestamp, method, request_path, body):
  return str(timestamp) + str.upper(method) + request_path + body

def parse_params_to_str(params):
    params = [(key, val) for key, val in params.items()]
    params.sort(key=lambda x: x[0])
    url = '?' +toQueryWithNoEncode(params);
    if url == '?':
        return ''
    return url

def toQueryWithNoEncode(params):
    url = ''
    for key, value in params:
        url = url + str(key) + '=' + str(value) + '&'
    return url[0:-1]

def create_signature(secret_key, timestamp, method, request_path, body=""):
    message = f"{timestamp}{method}{request_path}{body}"
    signature = hmac.new(secret_key.encode('utf-8'), message.encode('utf-8'), hashlib.sha256).hexdigest()
    return signature

def fetch_historical_data(ticker, bar_size, start_time, end_time):
    current_time = get_timestamp()
    params = {
        "symbol": ticker,
        "granularity": bar_size,
        "endTime": end_time,
        "startTime": start_time,
        "limit": 200  # Maximum records per request
    }

    method = "GET"
    signature = create_signature(SECRET_KEY, current_time, method, ENDPOINT + parse_params_to_str(params))    

    headers = {
        "ACCESS-KEY": API_KEY,
        "ACCESS-TIMESTAMP": str(current_time),
        "ACCESS-SIGN": signature,
        "ACCESS-PASSPHRASE": PASSPHRASE,
        "Content-Type": "application/json"
    }    

    response = requests.get(URL, headers=headers, params=params)

    data = response.json().get("data", []) 

    if response.status_code != 200:
        print(f"Error: {response.json()}", ticker)

    return data

current_time = datetime.now(timezone.utc)
start_time_days = 1
end_time_days = 4
bar_size = "1h"
ticker_list = ["BTCUSDT", "ETHUSDT", "LTCUSDT"]

columns_dict = {0:"timestamp", 1:"open", 2:"high", 3:"low", 4:"close", 5:"volume_base", 6:"vol_quote", 7: "volume_quote"}
start_time = int((current_time - timedelta(days=start_time_days)).timestamp() * 1000) 
end_time = int((current_time - timedelta(days=end_time_days)).timestamp() * 1000) 

for ticker in ticker_list:
    end_time_var = end_time
    filename = str(dt.datetime.fromtimestamp(start_time/1000.0))[:-6].replace(":",".").replace("-","") + "-" + str(dt.datetime.fromtimestamp(end_time/1000.0))[:-6].replace(":",".").replace("-","")+"_"+ticker+"_"+bar_size+"_bgt_ohlc.csv"    
    result = pd.DataFrame()   
    while end_time_var < start_time:
        result = result._append( fetch_historical_data(ticker, bar_size, start_time, end_time_var) ) 
        end_time_var += 691200000 #ms in 8 days, max returned bars of 1h
        time.sleep( random.randint(5,25)/100 ) #rate limit is 20 calls in 1 second
    
    try: 
        result.rename(columns=columns_dict, inplace=True)
        result.drop_duplicates(inplace=True)
        result["date"] = result["timestamp"].apply( datetime_format )
        result.set_index("date", inplace=True)
        result.sort_index(inplace=True) 
        result.to_csv(filename)
    except:
        pass    
    print(result.head(3), result.tail(3))