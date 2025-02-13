import requests
import time
import hmac
import hashlib
import base64
import json
import datetime as dt 
from datetime import datetime, timedelta, timezone

pk = open("key_bg.txt","r")
keys = pk.readlines()
API_KEY = keys[0].strip()
SECRET_KEY = keys[1].strip()
PASSPHRASE = keys[2].strip()
BASE_URL = "https://api.bitget.com"
ENDPOINT = "/api/v2/account/all-account-balance" 
URL = BASE_URL + ENDPOINT

def get_timestamp():
  return int(time.time() * 1000)

def sign(message, secret_key):
  mac = hmac.new(bytes(secret_key, encoding='utf8'), bytes(message, encoding='utf-8'), digestmod='sha256')
  d = mac.digest()
  return base64.b64encode(d)

def pre_hash(timestamp, method, request_path):
  return str(timestamp) + str.upper(method) + request_path 

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

def fetch_account_bals_v2():
    current_time = get_timestamp()    
    method = "GET"
    signature =  sign(pre_hash(current_time, "GET", ENDPOINT), SECRET_KEY)
    headers = {
        "ACCESS-KEY": API_KEY,
        "ACCESS-TIMESTAMP": str(current_time),
        "ACCESS-SIGN": signature,
        "ACCESS-PASSPHRASE": PASSPHRASE,
        "Content-Type": "application/json"
    }       
    
    response = requests.get(URL, headers=headers)

    data = response.json().get("data", []) 

    if response.status_code != 200:
        print(f"Error: {response.json()}")

    return data

def fetch_account_bals_v1():
    current_time = get_timestamp()    
    method = "GET"   
    signature =  sign(pre_hash(current_time, "GET", ENDPOINT), SECRET_KEY)     
    headers = {
        "ACCESS-KEY": API_KEY,
        "ACCESS-TIMESTAMP": str(current_time),
        "ACCESS-SIGN": signature,
        "ACCESS-PASSPHRASE": PASSPHRASE,
        "Content-Type": "application/json"
    }       
    
    response = requests.get(URL, headers=headers)

    data = response.json().get("data", []) 

    if response.status_code != 200:
        print(f"Error: {response.json()}")

    return data

if __name__ == '__main__':

    try:
        account_balances = fetch_account_bals_v2()
        print(account_balances)
    except Exception as e:
        print(f"Error: {e}")



