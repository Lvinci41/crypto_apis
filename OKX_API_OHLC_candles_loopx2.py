import base64
import datetime as dt
from datetime import datetime, timedelta, timezone
import hmac
import requests
import pandas as pd
import time
import json
import csv
import random

pk = open("pk_OKX_read.txt","r")
keys = pk.readlines()
APIKEY = keys[0].strip()
APISECRET = keys[1].strip()
PASS = keys[2].strip()
BASE_URL = "https://www.okx.com/api/v5/market/history-candles"

def fetch_okx_ohlc_data(api_key, secret_key, passphrase, instrument_id, bar, start_time, end_time):

    headers = {
        "OK-ACCESS-KEY": api_key,
        "OK-ACCESS-PASSPHRASE": passphrase,
    }
    
    all_data = []
    
    params = {
        "instId": instrument_id,
        "after": start_time,
        "before": end_time,
        "bar": bar,
        "limit": 100  
    }
    
    response = requests.get(BASE_URL, headers=headers, params=params)
    
    if response.status_code != 200:
        print(f"Error: {response.json()}")
    
    data = response.json().get("data", [])
    all_data.extend(data)
    
    columns = ["timestamp", "open", "high", "low", "close", "volume", "volume_ccy", "volCcyQuote", "confirm"]
    df = pd.DataFrame(all_data, columns=columns)

    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
    df.set_index("timestamp", inplace=True)
    
    return df    

current_time = datetime.now(timezone.utc)
start_time_days = 1
end_time_days = 2700
bar_size = "1H"
ticker_list = ["BTC-USDT", "ETH-USDT", "LTC-USDT", "XRP-USDT", "DOGE-USDT","BTC-USDC", "ETH-USDC", "LTC-USDC", "XRP-USDC", "DOGE-USDC"]

columns = ["timestamp", "open", "high", "low", "close", "volume", "volume_ccy", "volCcyQuote", "confirm"]
start_time = int((current_time - timedelta(days=start_time_days)).timestamp() * 1000) 
end_time = int((current_time - timedelta(days=end_time_days)).timestamp() * 1000) 

for ticker in ticker_list:
    start_time_var = start_time 
    filename = str(dt.datetime.fromtimestamp(start_time/1000.0)).replace(":",".")[:-6].replace(":",".").replace("-","") + "-" + str(dt.datetime.fromtimestamp(end_time/1000.0)).replace(":",".")[:-6].replace(":",".").replace("-","")+"_"+ticker+"_"+bar_size+"_ohlc.csv"    
    result = pd.DataFrame(columns=columns)
    result.set_index("timestamp", inplace=True)    
    while start_time_var > end_time:
        result = result._append( fetch_okx_ohlc_data(APIKEY, APISECRET, PASS, ticker, bar_size, start_time_var, end_time) )
        start_time_var -= 345600000 #ms in 4 days, max returned bars of 1h
        time.sleep( random.randint(10,35)/100 ) #rate limit is 20 calls in 2 seconds

    result.drop_duplicates(inplace=True)
    result.sort_index(inplace=True)
    result.to_csv(filename)
    print(result.head(), result.tail())
