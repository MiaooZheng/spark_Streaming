from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf, from_utc_timestamp
from pyspark.streaming import StreamingContext
from pyspark.sql.window import Window 
from pyspark.sql import functions as F
import logging



import pandas as pd
import statsmodels.api as sm
from pmdarima import auto_arima


import plotly.express as px
from dash import Dash, dcc, html, Input, Output
import plotly.express as px

from create_spark_session import create_session

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')
logger = logging.getLogger("spark_structured_streaming")

hostname = "127.0.0.1"
port = "9042"
ticker = "AAPL" # for test



def processed_in_spark(hostname, port):
    try:
        spark = create_session()
        
        spark.sparkContext.setLogLevel("ERROR")
        logging.info('Spark session created successfully')
    except Exception:
        logging.error("Couldn't create the spark session")
                
        
    try:
            # read data from Cassandra table into a Spark DataFrame
            spark_df = spark.read \
                .format("org.apache.spark.sql.cassandra") \
                .options(table=f"daily_stock_price", keyspace="dev") \
                .option("spark.cassandra.connection.host", f"{hostname}") \
                .option("spark.cassandra.connection.port", f"{port}") \
                .load()
            
            # spark_df = spark_df.withColumn('date', col('date').cast(TimestampType()))


            # Then we'll plot -> just a test at this moment
            # spark_df = spark_df.withColumn('date', col('date').cast(TimestampType()))
            ma5 = Window.partitionBy("company").orderBy("date").rowsBetween(-5, 0)
            spark_df = spark_df.withColumn("ma5_close", F.avg("close").over(ma5))
            ma30 = Window.partitionBy("company").orderBy("date").rowsBetween(-30, 0)
            spark_df = spark_df.withColumn("ma30_close", F.avg("close").over(ma30))
            ma365 = Window.partitionBy("company").orderBy("date").rowsBetween(-365, 0)
            spark_df = spark_df.withColumn("ma365_close", F.avg("close").over(ma365))


            spark_df = spark_df.orderBy('date', ascending=False)
            

            spark_df.show(10) # just in order to check whethe it works

            return spark_df
            
 
    except Exception as e:
            logging.error(f"Error reading data from Cassandra. Error: {e}")
            return None

    
processed_in_spark(hostname, port)