# Big Data Project

## Problem

## Project Architecture

1. **Data Streaming**:  
   - Kafka streams real-time data.  

2. **Storage**:  
   - Data is stored in HDFS for durability.  

3. **Batch Processing**:  
   - Spark processes data in batches and pushes results to Elasticsearch.  

4. **Visualization**:  
   - Kibana provides an interactive dashboard for data analysis.


## Getting Started

### 1. Clone the repository
```bash
git clone https://github.com/cbien0504/big-data  

cd big-data
```

### 2. Start the services
Use Docker Compose to bring up all the required services. 
```bash 
docker compose up -d
```

### 3. Stream data to Kafka
Run the following command to start streaming data:  
```bash
docker exec spark-master python3 kafa-streaming kafka_streaming.py
```
- Kafka Broker UI: http://localhost:9021

### 4. Stream data from Kafka to HDFS
Run the Spark job to stream data from Kafka to HDFS:  
```bash 
docker exec spark-master spark-submit spark-jobs/spark-streaming.py  
```
- HDFS UI: http://localhost:9870


### 5. Batch processing
Run the Spark job for batch processing and storing results in Elasticsearch:  
```bash 
docker exec spark-master spark-submit spark-jobs/batch_processing.py  
```
- Elasticsearch API: http://localhost:9200


### 6. Visualize data in Kibana
Open Kibana to explore and visualize the processed data:  

- Kibana Dashboard: http://localhost:5601

---