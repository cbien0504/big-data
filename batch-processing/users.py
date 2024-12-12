from pyspark.sql import SparkSession
import logging
def create_spark_connection():
    try:
        spark = SparkSession.builder \
            .appName('SparkToElasticSearch') \
            .config("spark.es.nodes", "localhost") \
            .config("spark.es.port", 9200) \
            .config("spark.jars", "/es/elasticsearch-spark-20_2.12-7.13.1.jar") \
            .config("spark.jars.packages", "org.apache.httpcomponents:httpclient:4.5.14") \
            .config("spark.es.index.auto.create", "true") \
            .config("spark.ui.port", "4045") \
            .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
        print("Spark connection created successfully!")
        return spark
    except Exception as e:
        logging.error(f"Couldn't create the Spark session due to exception: {e}", exc_info=True)
        return None

def read_from_hdfs(spark):
    hdfs_path = "hdfs://namenode:9000/user/hdfs/users/*.json"
    try:
        df = spark.read.format("json").load(hdfs_path)
        df.printSchema()
        logging.info("DataFrame loaded successfully from HDFS.")
        print("DataFrame loaded successfully from HDFS.")
        return df
    except Exception as e:
        logging.error(f"Error while reading data from HDFS: {e}", exc_info=True)
        return None

def write_to_elasticsearch(df):
    try:
        df.write.format("org.elasticsearch.spark.sql")\
            .mode("append")\
            .option("es.nodes", "http://elasticsearch:9200")\
            .option("es.nodes.discovery", "false")\
            .option("es.nodes.wan.only", "true")\
            .option("es.index.auto.create", "true")\
            .option("es.mapping.id", "_id")\
            .option("es.mapping.exclude", "_id")\
            .option("es.resource", "index_users")\
            .save()
        logging.info("DataFrame written successfully to Elasticsearch.")
        print("DataFrame written successfully to Elasticsearch.")
    except Exception as e:
        logging.error(f"Error while writing to Elasticsearch: {e}", exc_info=True)
        print("Error while writing to Elasticsearch")
        
if __name__ == "__main__":
    spark = create_spark_connection()
    if spark:
        df = read_from_hdfs(spark)
        if df:
            write_to_elasticsearch(df)
