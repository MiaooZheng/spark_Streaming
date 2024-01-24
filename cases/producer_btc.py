import numpy as np
import pandas as pd
from datetime import datetime, timedelta, timezone
import json
import time
from kafka import KafkaProducer
import requests
import pytz
from json import dumps


def extract_btc_producer():
    url = f"https://www.bitstamp.net/api/v2/ohlc/btcusd/"
    params = {
        "step": "60",
        "limit":  1,#int(num_bars) + 14,
    }
    
    data = requests.get(url, params=params).json()["data"]["ohlc"]
    data = pd.DataFrame(data)
    data.timestamp = pd.to_datetime(data.timestamp.astype(int), unit = "s")
    data['timestamp'] = data['timestamp'].dt.tz_localize('GMT')
    data['timestamp'] = data['timestamp'].dt.tz_convert('America/Toronto').astype(str)

    print(data)


    # ----------------------------send the streaming data into kafka topic----------------------------------
    bootstrap_servers = 'localhost:9092'
    kafka_topic = 'btc3' 
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         value_serializer=lambda x: dumps(x).encode('utf-8'))
    try:
        dict_stock = data.iloc[0].to_dict()
        print(dict_stock)
        producer.send(kafka_topic, value=dict_stock)
        producer.flush()

        print("Data sent to Kafka topic:", kafka_topic)

    except Exception as e:
        print(f"Error: {e}")

    


# sent data every second
while True:
  extract_btc_producer()
  time.sleep(1) #