#Programming Assignment for Friedman LLP Interview
#Author: Spencer Tasso
#Date: 8/14/2021

import datetime as dt
import pandas as pd
import numpy as np
from datetime import datetime 
from binance.client import Client
from pathlib import Path

#-------------------------------------------------------------------------------------------------------------
#pulls candlestick data from Binance.us, then write the data to a .csv file
def create_csv(client, symbol, filename, labels, first_of_month, yesterday) :
    print(f"creating {filename}...")
    
    candles = client.get_historical_klines(symbol, Client.KLINE_INTERVAL_15MINUTE, first_of_month, yesterday)
    
    candles_df = pd.DataFrame(candles, columns=labels)
    candles_df.to_csv(filename)

    print(f"Done.")

#-------------------------------------------------------------------------------------------------------------
#checks to see if the .csv file has the most recent data, if it does not, appends the new data to the .csv.
def append_to_csv(client, symbol, filename, labels, yesterday) :
    print(f"adding new data to {filename}...")

    from_csv = pd.read_csv(filename)
    last_date_csv = str(from_csv['Open time'].iloc[-1]) #gets the date of the most recent entry in the .csv
    last_date_obj = dt.datetime.utcfromtimestamp(float(last_date_csv) / 1000) 
    yesterday_obj = dt.datetime.fromtimestamp(float(yesterday) / 1000)
    last_date_csv = str(last_date_obj.timestamp() * 1000)

    if(last_date_obj.date() == yesterday_obj.date()) :
        print(f"{filename} is already up to date\nDone.")
        return  

    candles = client.get_historical_klines(symbol, Client.KLINE_INTERVAL_15MINUTE, last_date_csv, yesterday)
    
    candles_df = pd.DataFrame(candles, columns=labels) 
    candles_df.drop(index=candles_df.index[0], axis=0, inplace=True) #removes first row of the dataframe
    #changes the row indexs of the dataframe to match what is already in the .csv file
    candles_df = candles_df.set_index(np.arange(len(from_csv.index),len(from_csv.index)+len(candles_df.index)))

    candles_df.to_csv(filename, mode='a', header=False) 
    print(f"Done.")  

#-------------------------------------------------------------------------------------------------------------
#generates a milisecond timestamp for 11:59:59 last night
def get_yesterday_timestamp():
    before_midnight = datetime.max.time()
    yesterday_d = dt.date.today() - dt.timedelta(days=1) 
    yesterday_dt = datetime.combine(yesterday_d, before_midnight)

    return str(yesterday_dt.timestamp() * 1000)

#-------------------------------------------------------------------------------------------------------------
#generates a milisecond timestamp for midnight on the first day of the current month
def get_first_of_month_timestamp():
    midnight = datetime.min.time()
    first_of_month_d = dt.date.today().replace(day=1)
    first_of_month_dt = datetime.combine(first_of_month_d, midnight)
    
    return str(first_of_month_dt.timestamp() * 1000)

API_KEY = '8Ji91hqp6yuLbnRwKPFWQB8aUyY5SeuhJ5TbjQXVDNp40mVoR9dnEk8ybfI2r70T'
API_SECRET_KEY = 'vmcyPRQIc51Pd8HX9VWVJhv1MQ4cI0vuKBYnhHDC6I3LBtXyQTymQXgTrNg6nMc0'  
btc_filename = 'btc.csv'
eth_filename = 'eth.csv'
labels = ["Open time", 'Open', 'High', 'Low', 'Close', 'Volume', "Close time", "Quote asset volume",
"Number of trades", "Taker buy base asset volume", "Taker buy quote asset volume", 'Ignore']     
client = Client(API_KEY, API_SECRET_KEY)

if Path(btc_filename).is_file():
    print ("Bitcoin File exists.")
    append_to_csv(client, 'BTCUSDT', btc_filename, labels, get_yesterday_timestamp())
    
else:
    print ("Bitcoin File does not exist.")
    create_csv(client, 'BTCUSDT', btc_filename, labels, get_first_of_month_timestamp(), get_yesterday_timestamp())

if Path(eth_filename).is_file():
    print ("Etherium File exists.")
    append_to_csv(client, 'ETHUSDT', eth_filename, labels, get_yesterday_timestamp())

else:
    print ("Etherium File does not exist.")
    create_csv(client, 'ETHUSDT', eth_filename, labels, get_first_of_month_timestamp(), get_yesterday_timestamp())