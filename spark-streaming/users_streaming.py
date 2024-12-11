import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType
from pyspark.sql import functions as F
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from define_schema.define_schema import UserDocument
def create_spark_connection():
    try:
        spark = SparkSession.builder \
            .appName('KafkaToHDFS') \
            .config('spark.hadoop.hadoop.security.authentication', 'simple') \
            .config("spark.hadoop.dfs.replication", "1") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1") \
            .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
        print("Connected to HDFS successfully")
        return spark
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception: {e}")
        return None
def connect_to_kafka(spark):
    try:
        kafka_df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "broker:29092") \
            .option("subscribe", "users_topic") \
            .option("multiline", "true")\
            .load()
        logging.info("Kafka DataFrame created successfully")
        print("Kafka DataFrame created successfully")
        return kafka_df
    except Exception as e:
        logging.error(f"Kafka DataFrame could not be created: {e}", exc_info=True)
        return None
def create_selection_df(kafka_df):
    users_schema = UserDocument.get_schema()
    selection_df = kafka_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), users_schema).alias("data")) \
        .select("data.*")
    return selection_df

def write_to_hdfs(selection_df):
    try:
        streaming_query = selection_df.writeStream \
            .outputMode("append") \
            .format("json") \
            .option("path", "hdfs://namenode:9000/user/hdfs/users/") \
            .option("checkpointLocation", "hdfs://namenode:9000/user/hdfs/users_checkpoint/") \
            .start()
        logging.info("Writing data to HDFS")
        print("Writing data to HDFS")
        streaming_query.awaitTermination()
    except Exception as e:
        logging.error(f"Failed to write to HDFS: {e}", exc_info=True)
        print("Failed to write to HDFS")
if __name__ == "__main__":
    spark = create_spark_connection()
    kafka_df = connect_to_kafka(spark)
    if kafka_df is not None:
        selection_df = create_selection_df(kafka_df)
        write_to_hdfs(selection_df)