import requests
import pandas as pd
import pymysql
import os
import datetime
from datetime import datetime


def request_api(url, params=None, key=None):

   if params is None:
      response = requests.get(url)
      data = response.json()
   elif params is not None:
      response = requests.get(url, params= params)
      data = response.json()[key]

   df = pd.DataFrame(data) 
   return df

print(" - Extraction process initialized - ")

# Url coin list
coins_url = "https://api.coingecko.com/api/v3/coins/list"
df_coins_list=request_api(coins_url, params=None)
bitcoin_id = df_coins_list.loc[df_coins_list["id"] == "bitcoin", "id"].iloc[0]


# Url historical market data 
url_currency_historical_market_data = "https://api.coingecko.com/api/v3/coins/{}/market_chart/range"
#Set date range
initial_date = int(datetime.strptime("2022-01-01", "%Y-%m-%d").timestamp())
final_date = int(datetime.strptime("2022-03-31", "%Y-%m-%d").timestamp())
#Params for requets
params = {
    "vs_currency": "usd",
    "from": initial_date,
    "to": final_date,
}

df_bitcoin_history_market = request_api(url_currency_historical_market_data.format(bitcoin_id), params = params, key="prices")

#Rename columns
df_bitcoin_history_market.rename(columns= {0:'date',1:'usd_price'}, inplace = True)
#Format the column date as 'datetime64[ns]'
df_bitcoin_history_market['date']=df_bitcoin_history_market['date'].astype('datetime64[ns]')
#Format the column usd price as float
df_bitcoin_history_market['usd_price']=df_bitcoin_history_market['usd_price'].astype(float)



#Credentials db
host=""
user=""
password=""

conn = pymysql.connect(host=host,
                        user=user,
                        passwd=password,
                        connect_timeout=30)

cursor = conn.cursor()


query_create_database="""CREATE DATABASE IF NOT EXISTS currencies;"""        
cursor.execute(query_create_database)
conn.commit()


#Create table
query_create_table = """CREATE TABLE IF NOT EXISTS currencies.bitcoin_market_history_price(
                                id INTEGER PRIMARY KEY AUTO_INCREMENT,
                                date DATETIME NOT NULL, 
                                usd_price DECIMAL(10, 2) NOT NULL
                                );"""
cursor.execute(query_create_table)
conn.commit()


data_bitcoin_history_market= list(zip(df_bitcoin_history_market['date'], df_bitcoin_history_market['usd_price']))
query_insert_history_market= """INSERT INTO currencies.bitcoin_market_history_price(date, usd_price) VALUES (%s, %s);"""
cursor.executemany(query_insert_history_market, data_bitcoin_history_market)
conn.commit()
print(" - Data saved into table bitcoin_market_history_price successfully - ")

query_consult_all_data ="""SELECT * FROM currencies.bitcoin_market_history_price;"""  
cursor.execute(query_consult_all_data)
data_history_market_1T = cursor.fetchall()


df_history_market_1T = pd.DataFrame(data = data_history_market_1T, columns= ["id","date","usd_price"])
df_history_market_1T["usd_price"] = df_history_market_1T["usd_price"].astype(float)
df_history_market_1T['price_mean_partition_five_days'] = df_history_market_1T['usd_price'].rolling(window=5).mean()
print(df_history_market_1T)


