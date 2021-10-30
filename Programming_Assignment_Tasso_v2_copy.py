import requests
import datetime as dt
import csv
from pathlib import Path


#-------------------------------------------------------------------------------------------------------------
#generates a milisecond timestamp for 11:45:00 last night
def get_15_before_midnight():
    before_midnight = dt.datetime.max.time() 
    yesterday_d = dt.date.today() - dt.timedelta(days=1) 
    yesterday_dt = dt.datetime.combine(yesterday_d, before_midnight)
    yesterday_dt -= dt.timedelta(seconds=59, minutes=14, milliseconds=999)
    fifteen_before = str(yesterday_dt.timestamp() * 1000)

    if len(fifteen_before) > 13 :
        return fifteen_before[:13]
    
    return fifteen_before
#-------------------------------------------------------------------------------------------------------------
#generates a milisecond timestamp for 11:59:59 last night
def get_yesterday_timestamp():
    before_midnight = dt.datetime.max.time()
    yesterday_d = dt.date.today() - dt.timedelta(days=1) 
    yesterday_dt = dt.datetime.combine(yesterday_d, before_midnight)
    yesterday_ts = str(yesterday_dt.timestamp() * 1000)

    if len(yesterday_ts) > 13 :
        return yesterday_ts[:13]
    
    return yesterday_ts

#-------------------------------------------------------------------------------------------------------------
#generates a milisecond timestamp for midnight on the first day of the current month
def get_first_of_month_timestamp():
    midnight = dt.datetime.min.time()
    first_of_month_d = dt.date.today().replace(day=1)
    first_of_month_dt = dt.datetime.combine(first_of_month_d, midnight)
    first_of_month_ts = str(first_of_month_dt.timestamp() * 1000)

    if len(first_of_month_ts) > 13 :
        return first_of_month_ts[:13]
    
    return first_of_month_ts
#-------------------------------------------------------------------------------------------------------------
#pulls candlestick data from Binance.us, then write the data to a .csv file
def get_data(symbol, start_time) :
    API_KEY = '8Ji91hqp6yuLbnRwKPFWQB8aUyY5SeuhJ5TbjQXVDNp40mVoR9dnEk8ybfI2r70T'
    j = []
    params = {
        'symbol': 'null',
        'interval': '15m',
        'startTime' : 'null' ,
        'endTime' : get_yesterday_timestamp(),
        'limit' : 1000
    }
    headers = {
    'X-MBX-APIKEY': API_KEY
    }
    params['symbol'] = symbol
    params['startTime'] = start_time 
    print(params)

    lc = 0
    while(True) :
        r = requests.get('https://api.binance.us/api/v3/klines', params=params, headers=headers)
        j += r.json()
        params["startTime"] = j[-1][0]
        
        if(str(j[-1][0]) == get_15_before_midnight()) :
            break
        lc+=1
        print(lc)
        j.pop()
    
    return j
def create_csv(symbol, filename) :
    data = get_data(symbol, get_first_of_month_timestamp())

    with open(filename, "w", newline='') as f:
        writer = csv.writer(f)
        writer.writerows(data)
        
#-------------------------------------------------------------------------------------------------------------
#checks to see if the .csv file has the most recent data, if it does not, appends the new data to the .csv.
def append_to_csv( symbol, filename) :
    API_KEY = '8Ji91hqp6yuLbnRwKPFWQB8aUyY5SeuhJ5TbjQXVDNp40mVoR9dnEk8ybfI2r70T'
   # j = []
    params = {
        'symbol': 'null',
        'interval': '15m',
        'startTime' : 'null',
        'endTime' : get_yesterday_timestamp(),
        'limit' : 1000
    }
    headers = {
    'X-MBX-APIKEY': API_KEY
    }
    file = open(filename, "r")
    input = file.readlines()
    last_date_csv = input[-1].partition(',')[0]
    last_date_obj = dt.datetime.fromtimestamp(float(last_date_csv) / 1000) 
    yesterday_obj = dt.datetime.fromtimestamp(float(get_yesterday_timestamp()) / 1000)

    if(last_date_obj.date() == yesterday_obj.date()) :
        print(f"{filename} is already up to date\nDone.")
        return 

    params['symbol'] = symbol
    params['startTime'] = last_date_csv  
    
    

#    params = {
#         'symbol': 'BTCUSDT',
#         'interval': '15m',
#         'startTime' : get_first_of_month_timestamp(),
#         'endTime' : get_yesterday_timestamp(),
#         'limit' : 1000
#    }

# print(params)
# params["startTime"] = 484884488485555555555

# print(params)

create_csv('BTCUSDT','btcv2.csv')
# create_csv('ETHUSDT','ethv2.csv')
# append_to_csv('BTCUSDT', 'btcv2.csv')
#print(json)
# print(len(json))
# print(json[0][0])
# print(json[2000][0])
# print(json[2001][0])

# print(json[-1][0])
# print(1000 % 1000)
# #print(json[1001])
# #print(json[1500])
# print(f"first day of month = {get_first_of_month_timestamp()}")
# print(f"yesterday= {get_yesterday_timestamp()}")
# print(f"fifteen before= {get_15_before_midnight()}")



# response = requests.get('https://api.binance.us')
# response.raise_for_status()
# # access JSOn content
# jsonResponse = response.json()
# print("Entire JSON response")
# pprint.pprint(jsonResponse)
