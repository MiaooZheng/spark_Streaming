from pyspark.sql import SparkSession   
import logging

# here we create a spark session so streaming case and historical case can use the same one


logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')
logger = logging.getLogger("spark_structured_streaming")


def create_session():
    hostname = "127.0.0.1"
    port = "9042"

    spark = SparkSession.builder \
    .appName("SparkConnection") \
    .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.0.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
    .config("spark.cassandra.connection.host", f"{hostname}") \
    .config("spark.cassandra.connection.port", f"{port}") \
    .config("spark.sql.session.timeZone", "UTC") \
    .config("spark.ui.reverseProxy", "true") \
    .getOrCreate()
    return spark
            
    
create_session()