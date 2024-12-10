from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .appName('KafkaToHDFS') \
    .config('spark.hadoop.hadoop.security.authentication', 'simple') \
    .config("spark.hadoop.dfs.replication", "1") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1") \
    .getOrCreate()
df = spark.read.format("json").load("hdfs://namenode:9000/user/hdfs/projects/*.json")
df.show(5,truncate = False)