import numpy as np
import pandas as pd
import yfinance as yf
from pprint import pprint
from datetime import datetime, timedelta, timezone
from kafka import KafkaProducer

import warnings
warnings.filterwarnings("ignore")


# NOTE: WE'LL SAVE ALL COMPANY DATA INTO THE SAME TABL called daily_stock_price




####  STOCK MARKET ONLY OPENS FROM MONDAY TO FRIDAY
# NEED TO ADD CASE WHEN WE WANNA EXTRACT ON WEEKEND (done)
def extract_daily_data(ticker):
  # Fetch historical data
  today = datetime.now(timezone(timedelta(hours=-5))).strftime('%Y-%m-%d')
  end_day = datetime.strptime(today, "%Y-%m-%d")
  data = yf.download(tickers=ticker, interval='1d', end = end_day)
  data = data.reset_index()

  # data['Date'] = data['Date'].apply(lambda x: datetime.utcfromtimestamp(x.timestamp()).strftime('%Y-%m-%d'))
  data['Company'] = ticker
  # data['Date'] = pd.to_datetime(data['Date'])
  # data.to_csv(f"{ticker}_daily_stock_prices_data.csv")
  # print(data)
  return data

print(extract_daily_data(ticker ='AAPL'))



from cassandra.cluster import Cluster

# when i run cqls command -> got: Connected to Test Cluster at 127.0.0.1:9042 -> 9042 is port
def connect_to_cassandra():
    # Connect to the Cassandra cluster
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect() 

    # then we need to create a keyspace before create a table -> so keyspace is like a dataset in relational db
    keyspace_name = 'dev'
    replication_config = {
    'class': 'SimpleStrategy',
    'replication_factor': 1   # a bit confused, but let's test
    }

    session.execute(f"""
    CREATE KEYSPACE IF NOT EXISTS {keyspace_name}
    WITH replication = {replication_config}
    """)

    # switch to the dev keyspace
    session = cluster.connect('dev')

    # then we can create table
    # primary key:  # the first one is partition_key and after that is cluster key
    session.execute(f"""
    CREATE TABLE IF NOT EXISTS daily_stock_price (
        Date timestamp,
        Company text,
        Open float,
        High float,
        Low float,
        Close float,
        Adj_close float,
        Volume bigint,
        PRIMARY KEY (Company, Date)
    );

    """)

    # TRUNCATE TABLE BEFORE INSERT NEW DATA
    session.execute(f"""
                    TRUNCATE TABLE  {keyspace_name}.daily_stock_price
                    """)

    print("truncate table successfully!")
    
    # read data

    tickers = ["AAPL", "META", "NFLX", "AMZN", "GOOGL", "^IXIC", "UBER", "COIN", "RIOT", "GME"]

    for ticker in tickers:
        data = extract_daily_data(ticker)
        print(data.head(1))


        # In Cassandra, seems like we can only insert row by row. cannot insert a whole df 
        
        
        query = f"""
            INSERT INTO {keyspace_name}.daily_stock_price (Date, Company, Open, High, Low, Close, Adj_Close, Volume)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """
        prepared = session.prepare(query)
        
        for index, row in data.iterrows():
            session.execute(prepared,
            (row['Date'].tz_localize('UTC'), # need to follow UTC as well, in order to avoid date and values mismatch
            row['Company'],
            row['Open'],
            row['High'],
            row['Low'],
            row['Close'],
            row['Adj Close'],
            row['Volume'],
            
            ))
    print("saved in db successfully!")


    result = session.execute(f"""SELECT * FROM {keyspace_name}.daily_stock_price
                             WHERE Company = '{ticker}'
                             ORDER BY Date""")

    # convert to df 
    df = pd.DataFrame(result.all())

    
    print(df) #### NOTE: HERE THE DATETIME IS EST - NOT SURE WHETHER THERE'S ANY INFLUENCE FOR OUR CASE
    
    session.shutdown()
    cluster.shutdown()  
    

connect_to_cassandra()

## Probably we need to write a while loop to update table daily 
# or use airflow to update daily
