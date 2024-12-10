from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .appName('SparkToElasticSearch') \
    .config("spark.es.nodes", "localhost") \
    .config("spark.es.port", 9200) \
    .config("spark.jars", "es/elasticsearch-spark-20_2.12-7.13.1.jar") \
    .config("spark.jars.packages", "org.apache.httpcomponents:httpclient:4.5.14") \
    .config("spark.es.index.auto.create", "true") \
    .getOrCreate()

print(spark.version)
df = spark.read.format("json").load("test/files/*.json")
df.printSchema()

df.write.format("org.elasticsearch.spark.sql")\
    .option("es.nodes", "http://localhost:9200")\
    .option("es.nodes.discovery", "false")\
    .option("es.nodes.wan.only", "true")\
    .option("es.index.auto.create", "true")\
    .option("es.mapping.id", "_id")\
    .option("es.mapping.exclude", "_id")\
    .option("es.resource", "tweets")\
    .save()

print("DataFrame written successfully to Elasticsearch.")