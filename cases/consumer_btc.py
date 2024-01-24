from pyspark.sql import SparkSession
import time
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DoubleType
from pyspark.sql.functions import from_json, col, udf, avg, window, expr
from pyspark.streaming import StreamingContext
import logging
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from create_spark_session import create_session


#export KAFKA_HEAP_OPTS="-Xmx256M -Xms128M"

# run command below 
# # spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1,com.datastax.spark:spark-cassandra-connector_2.12:3.2.0 ./code/streaming_data_case/consumer_btc.py

def write_to_kafka(df, epoch_id):
    kafka_bootstrap_servers = "localhost:9092"
    kafka_output_topic = "streamingoutput2"

    # write the DataFrame to Kafka
    df.selectExpr("CAST(window AS STRING) AS key", "to_json(struct(*)) AS value") \
        .write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("topic", kafka_output_topic) \
        .option("checkpointLocation", "checkpoint") \
        .save()


def btc_streaming():
    spark = create_session()
    spark.sparkContext.setLogLevel("ERROR")

    kafka_params = {
        "kafka.bootstrap.servers": "localhost:9092",
        "subscribe": "btc3",
        "startingOffsets": "earliest",
        "failOnDataLoss": "false"  # handle data loss
}


    raw_stream_df = (
        spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_params["kafka.bootstrap.servers"])
        .option("subscribe", kafka_params["subscribe"])
        .option("startingOffsets", kafka_params["startingOffsets"])
        .option("failOnDataLoss", kafka_params["failOnDataLoss"]) 
        .load()
    )

    schema = StructType([
        StructField("close", StringType(), True),
        StructField("high", StringType(), True),
        StructField("low", StringType(), True),
        StructField("open", StringType(), True),
        StructField("volume", StringType(), True),
        StructField("timestamp", TimestampType(), True)])

    json_stream_df = raw_stream_df \
        .selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    json_stream_df = json_stream_df.select([col(c).alias(c.lower()) for c in json_stream_df.columns])

    stream_df = json_stream_df.select("timestamp", "high", "low", "open", "close", "volume")

    five_second_window = "5 seconds"

    moving_avg_df = stream_df \
        .withColumn("window", window("timestamp", five_second_window)) \
        .groupBy("window") \
        .agg(avg("close").alias("ma5secondsWindow"), expr("last(close, true)").alias("close"))

    moving_avg_df = moving_avg_df.sort("window")

  

    
    query = (
        moving_avg_df
        .writeStream
        .outputMode("complete")
        .trigger(processingTime="5 seconds")
        .foreachBatch(write_to_kafka)  
        .option("checkpointLocation", "checkpoint")
        .start()
    )
 
    '''
    query = (
    moving_avg_df
    .writeStream
    .outputMode("append")
    .trigger(processingTime="5 seconds")  
    .format("console")  
    .queryName("streaming_btc") 
    .option("checkpointLocation", "checkpoint")  # Checkpoint location for fault-tolerance
    .start()
    )

    '''

    # Wait for the termination of the streaming query
    query.awaitTermination()


btc_streaming()

