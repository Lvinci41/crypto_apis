import base64
import datetime as dt
from datetime import datetime, timedelta, timezone
import hmac
import requests
import pandas as pd
import time
import json
import csv

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
    df.sort_index(inplace=True)
    
    return df    

current_time = datetime.now(timezone.utc)
start_time = int((current_time - timedelta(days=2470)).timestamp() * 1000) 
end_time = int((current_time - timedelta(days=2474)).timestamp() * 1000) 

result = fetch_okx_ohlc_data(APIKEY, APISECRET, PASS, "BTC-USDT", "1H", start_time, end_time)
filename = str(start_time) + "-" + str(end_time)+'_btc_1h_ohlc.csv'

result.to_csv(filename)
print(result.head(), result.tail())