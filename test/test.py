from elasticsearch import Elasticsearch

es = Elasticsearch("http://localhost:9200") 
name = "index_projects"
if es.indices.exists(index=name):
    print("Index tồn tại.")
    response = es.search(
        index=name,
        body={
            "query": {"match_all": {}},  
            "size": 10              
        }
    )
    for hit in response['hits']['hits']:
        print(f"ID: {hit['_id']}, Source: {hit['_source']}")
else:
    print("Index không tồn tại.")
response = es.count(index=name)
print(f"Number of documents: {response['count']}")



# index_name = "tweets"
# if es.indices.exists(index=index_name):
#     response = es.indices.delete(index=index_name)
#     print(f"Index '{index_name}' đã được xóa: {response}")
# else:
#     print(f"Index '{index_name}' không tồn tại.")



# from pyspark.sql import SparkSession
# spark = SparkSession.builder.getOrCreate()
# df = spark.read.format("json").load("hdfs://namenode:9000/user/hdfs/users/*.json")
# df.printSchema()
# df.show()
# df.write.format("json").save("/test/files/users")