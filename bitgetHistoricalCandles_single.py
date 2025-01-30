import requests
import time
import hmac
import hashlib
import base64
import json
from datetime import datetime, timedelta

pk = open("pk_Bitget_read.txt","r")
keys = pk.readlines()
API_KEY = keys[0].strip()
SECRET_KEY = keys[1].strip()
PASSPHRASE = keys[2].strip()
BASE_URL = "https://api.bitget.com"
ENDPOINT = "/api/v2/spot/market/history-candles"
URL = BASE_URL + ENDPOINT

def get_timestamp():
  return int(time.time() * 1000)

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

def fetch_historical_data(ticker, bar_size, end_time):
    current_time = get_timestamp()
    params = {
        "symbol": ticker,
        "granularity": bar_size,
        "endTime": current_time,
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
        print(f"Error: {response.json()}")

    return data

print(fetch_historical_data("BTCUSDT", "4h", 1690196141868))