from pyspark.sql import SparkSession
import logging
def create_spark_connection():
    try:
        spark = SparkSession.builder \
            .appName('HDFSToElasticSearch') \
            .config('spark.hadoop.hadoop.security.authentication', 'simple') \
            .config("spark.es.nodes", "elasticsearch")  \
            .config("spark.es.port", "9200")  \
            .config("spark.es.index.auto.create", "true")  \
            .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
        print("Connected to HDFS successfully")
        return spark
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception: {e}")
        return None
def read_from_hdfs(spark):
    hdfs_path = "hdfs://namenode:9000/user/hdfs/tweets/*.json"
    try:
        df = spark.read \
            .format("json") \
            .option("multiline", "true") \
            .load(hdfs_path)
        
        logging.info("DataFrame loaded successfully from HDFS.")
        print("DataFrame loaded successfully from HDFS.")
        return df
    except Exception as e:
        logging.error(f"Error while reading data from HDFS: {e}", exc_info=True)
        return None
if __name__ == "__main__":
    spark = create_spark_connection()
    df = read_from_hdfs(spark)
    df.printSchema()