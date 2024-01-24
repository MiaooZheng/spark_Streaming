from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf, window
from pyspark.streaming import StreamingContext
import logging
from pyspark.sql.window import Window
from pyspark.sql import functions as F

#### NOTE: WE DIDN'T SHOW STREAMING CASE FOR YFINANCE 1MIN DATA IN OUR DASHBOARD, HERE IS JUST THE SAMPLE TO STORE IN CASSANDRA
# IF WE HAVE MORE TIME, WILL COME BACK TO FINISH

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')
logger = logging.getLogger("spark_structured_streaming")

hostname = "127.0.0.1"
port = "9042"
ticker = "AAPL" # for test

def writeToCassandra(df, epochId):
  df = df.withColumnRenamed("adj close", "adj_close")

  df.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table=f"{ticker.lower()}_stock_price", keyspace="dev") \
        .mode("append") \
        .option("spark.cassandra.connection.host", f"{hostname}") \
        .option("spark.cassandra.connection.port", f"{port}") \
        .save()

try:
  spark = SparkSession\
        .builder\
        .appName("KafkaStructuredStreaming")\
        .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.0.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
        .config("spark.cassandra.connection.host", f"{hostname}") \
        .config("spark.cassandra.connection.port", f"{port}") \
        .getOrCreate()
  
  spark.sparkContext.setLogLevel("ERROR")
  logging.info('Spark session created successfully')
except Exception:
  logging.error("Couldn't create the spark session")
        
 
        

bootstrap_servers = 'localhost:9092'
kafka_topic = 'streamingtest'

schema = StructType([
    StructField("Open",StringType(),True), 
    StructField("High", StringType(), True),
    StructField("Low", StringType(), True),
    StructField("Close", StringType(), True),
    StructField("Adj Close", StringType(), True),
    StructField("Volume", StringType(), True),
    StructField("Company", StringType(), True),
    StructField("Datetime", TimestampType(), True)])

# Define Kafka parameters
kafka_params = {
    "kafka.bootstrap.servers": "localhost:9092",
    "subscribe": "stockprice1",  # store_streaming_data # REPLACE WITH news at this moment 
    "startingOffsets": "earliest"  # Choose "earliest" if you want to process all available messages
  }

# Read data from Kafka as a streaming DataFrame
raw_stream_df = (
    spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafka_params["kafka.bootstrap.servers"])
    .option("subscribe", kafka_params["subscribe"])  # Ensure this line doesn't end with a backslash
    .option("startingOffsets", kafka_params["startingOffsets"])
    .load()
  )


# Parse the JSON data from the 'value' column
json_stream_df = raw_stream_df \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")


# change the order of column
json_stream_df = json_stream_df.select([col(c).alias(c.lower()) for c in json_stream_df.columns])

stream_df = json_stream_df.select("company", "datetime", "high", "low", "open", "close", "volume", "adj close")

'''
# streaming version1 without add to table -> this one works
stream_df.writeStream \
         .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .start()

'''


stream_df = stream_df.na.drop() # drop null in batch0

# stream_df.show()

# OPTIONAL 1: IF YOU WANNA STORE IN CASSANDRA -> RUN BELOW CODES
'''
stream_df.writeStream \
        .foreachBatch(writeToCassandra) \
        .outputMode("append") \
        .trigger(processingTime='60 seconds') \
        .start() \
        .awaitTermination()
'''

# OPTIONAL 2: IF YOU WANNA PERFORM MA ANALYSIS DIRECTLY ON SPARK STRUCTURED STREAMING
# do 5 minutes window MA on spark directly -> without storing in Cassandra db.


five_minute_window = "5 minutes" 

# Calculate moving average using window function
moving_avg_df = stream_df \
    .withColumn("window", window("datetime", five_minute_window)) \
    .groupBy("company", "window") \
    .agg(F.avg("close").alias("ma5minutesWindow"))

# Start streaming query
query = (
    moving_avg_df
    .writeStream
    .outputMode("complete")
    .format("console")  # Change this to your desired output sink
    .start()
)

# Wait for the termination of the streaming query
query.awaitTermination()



#### READ: https://kafka.apache.org/quickstart

# this is just for streaming command
# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 connect_to_sparkstreaming.py


# RUN COMMAND BELOW:
# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1,com.datastax.spark:spark-cassandra-connector_2.12:3.2.0 ./code/streaming_data_case/connect_to_sparkstreaming.py

'''
### CURRENT OUTPUT LOOKS LIKE: -> AND IT RUNS PER 1 SECOND, NOT A MINUTE

-------------------------------------------
Batch: 2
-------------------------------------------
+-------+-------------------+--------------+--------------+--------------+--------------+------+--------------+
|Company|Datetime           |High          |Low           |Open          |Close         |Volume|Adj Close     |
+-------+-------------------+--------------+--------------+--------------+--------------+------+--------------+
|AAPL   |2023-11-20 17:43:00|191.3800048828|191.3300018311|191.3699951172|191.3500061035|0     |191.3500061035|
+-------+-------------------+--------------+--------------+--------------+--------------+------+--------------+

'''


# reference: https://github.com/sebastianruizm/spark-kafka-cassandra/blob/main/scripts/python/data-pipeline-streaming.py