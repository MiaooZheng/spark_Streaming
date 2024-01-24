import numpy as np
import pandas as pd
import yfinance as yf
from pprint import pprint
from datetime import datetime, timedelta, timezone
import json 
import time
from confluent_kafka import Producer
from kafka import KafkaProducer

import warnings
warnings.filterwarnings("ignore")

# ------------------------------------------BEFORE RUNNING THE CODES -------------------------- # 
'''
# RUN COMMANDS BELOW IN YOUR TERMINAL

# Open the first terminal 

# download the kafka
wget https://archive.apache.org/dist/kafka/2.8.0/kafka_2.12-2.8.0.tgz
tar -xzf kafka_2.12-2.8.0.tgz

# start zookeeper 
cd kafka_2.12-2.8.0
bin/zookeeper-server-start.sh config/zookeeper.properties

-----------------------------------------------------------
# Open another terminal and run  commands below

# start kafka broker
cd kafka_2.12-2.8.0
bin/kafka-server-start.sh config/server.properties

# create a topic 

export KAFKA_HEAP_OPTS="-Xmx256M -Xms128M"
cd kafka_2.12-2.8.0
bin/kafka-topics.sh --create --topic stockprice1 --bootstrap-server localhost:9092

# start a producer 
bin/kafka-console-producer.sh --topic stockprice1 --bootstrap-server localhost:9092

-----------------------------------------------------------
# Open another terminal - for the consumer 
cd kafka_2.12-2.8.0
bin/kafka-console-consumer.sh --topic stockprice1 --from-beginning --bootstrap-server localhost:9092

'''
# ------------------------------------------ Start the coding part below-------------------------- #





####  STOCK MARKET ONLY OPENS FROM MONDAY TO FRIDAY
# NEED TO ADD CASE WHEN WE WANNA EXTRACT ON WEEKEND (done)
def extract_streaming_data(ticker, start_time = '09:30:00-05:00', close_time = '15:59:00-05:00'):

  # this current_day_time is what we will add to our final return 
  current_day_time = (datetime.now(timezone(timedelta(hours=-5))) - timedelta(minutes = 1)).strftime("%Y-%m-%d %H:%M:00-05:00")
  print("current_time in utc:", current_day_time)
  
  # if current_time > 4pm for each day, then return  15:59:00 data 
  current_day = datetime.now(timezone(timedelta(hours=-5))) # as we can get the last minute data for the latest one 
  print("weekday:", current_day.weekday())
  current_day_str = current_day.strftime("%Y-%m-%d")
  current_time_str = current_day.strftime("%H:%M:%S")
  print("current day time:", current_day)

  last_day =  current_day - timedelta(days = 1)
  last_day_str = last_day.strftime("%Y-%m-%d")
  print("last day:", last_day)

  # print(current_day)
  if current_day.weekday() >= 5:
    to_friday = current_day.weekday() - 4  # Adjust to Friday (4 is the weekday index for Friday)
    current_day -= timedelta(days=to_friday)
    current_day_str = current_day.strftime("%Y-%m-%d")
    end_time= current_day_str + ' ' + close_time
    print("friday close data:", end_time)
  elif current_time_str > close_time and current_day == current_day:
    end_time= current_day_str + ' ' + close_time
  elif current_time_str < start_time:
    end_time= last_day_str + ' ' + close_time
  else:
    end_time= (datetime.now(timezone(timedelta(hours=-5)))).strftime("%Y-%m-%d %H:%M:%S%z")
  
  end_time = datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S%z").replace(second=0)
  start_time = end_time - timedelta(minutes = 1)

  # Fetch data per 1 minute
  data = yf.download(tickers=ticker,  start= start_time, end = end_time, interval='1m')
  
  if not data.empty: 
    data['Datetime'] = current_day_time
    data['Company'] = ticker
    # json_data = data.to_json(orient='records')
  else:
    data = yf.download(tickers=ticker, interval='1m').tail(1)
    data['Datetime'] = current_day_time
    data['Company'] = ticker
    # json_data = data.to_json(orient='records')


  # send the streaming data into kafka topic 
  bootstrap_servers = 'localhost:9092'
  kafka_topic = 'stockprice1'  # Here our kafka topic called stockprice1


  # create a Kafka producer

  producer = KafkaProducer(bootstrap_servers=bootstrap_servers)


  try:
    key = "Company"
         
    for index, row in data.iterrows():
        data_to_send = row.to_json()
        producer.send(kafka_topic, value=data_to_send.encode('utf-8'))

    producer.flush()

    print(f"Data {data_to_send} sent to Kafka topic:", kafka_topic)

  except Exception as e:
     print(f"Error: {e}")


  return data


## run the codes for test: -> in this case it will send the latest data to kafka topic  per minute
while True:
  extract_streaming_data(ticker= 'AAPL')
  time.sleep(60) #

  
# our datetime (watermarks) is in the last column 


########

# WHEN WE RUN THIS FILE, PLEASE DOWNLOAD ZOOKEEPER AND SETUP KAFKA ON TERMINAL