import logging
from pyspark.sql import SparkSession


mongodb_url = "mongodb://dsReader:ds_reader_ndFwBkv3LsZYjtUS@178.128.85.210:27017,104.248.148.66:27017,103.253.146.224:27017"

def create_spark_connection():
    try:
        spark_conn = SparkSession.builder \
            .appName('MongoDBtoHDFS') \
            .config('spark.hadoop.hadoop.security.authentication', 'simple') \
            .config("spark.jars.packages", "mysql:mysql-connector-java:8.0.33") \
            .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
            .getOrCreate()

        spark_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
        print("Connected successfully")
        return spark_conn
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception: {e}")
        print("Error")
        return None

def create_table(connection):
    try:
        cursor = connection.cursor()
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS created_users (
            id VARCHAR(36) PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            gender TEXT,
            address TEXT,
            post_code TEXT,
            email TEXT,
            username TEXT,
            registered_date TEXT,
            phone TEXT,
            picture TEXT);
        """)
        connection.commit()
        cursor.close()
        print("Table created successfully!")
    except Error as e:
        logging.error(f"Error creating table: {e}")

def insert_data(connection, user_data):
    try:
        cursor = connection.cursor()
        insert_query = """
            INSERT INTO created_users (id, first_name, last_name, gender, address, 
                post_code, email, username, registered_date, phone, picture)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(insert_query, user_data)
        connection.commit()
        cursor.close()
        logging.info(f"Data inserted for {user_data[1]} {user_data[2]}")
        print("Inserted data")
    except Error as e:
        logging.error(f"Could not insert data due to {e}")

def read_parquet_from_hdfs(spark_conn, hdfs_path):
    try:
        df = spark_conn.read.parquet(hdfs_path)
        logging.info("Parquet files read successfully")
        return df
    except Exception as e:
        logging.error(f"Failed to read Parquet files from HDFS: {e}")
        print("Error")
        return None

if __name__ == "__main__":
    hdfs_path = "hdfs://namenode:9000/user/hdfs/created_users"
    spark_conn = create_spark_connection()
    if spark_conn:
        parquet_df = read_parquet_from_hdfs(spark_conn, hdfs_path)
        if parquet_df:
            parquet_df.show()
            parquet_df.printSchema()

            # Connect to MySQL
            try:
                connection = mysql.connector.connect(
                    host='mysql',
                    database='mydatabase',
                    user='admin',
                    password='admin'
                )

                if connection.is_connected():
                    create_table(connection)
                    # Insert data from DataFrame
                    for row in parquet_df.collect():
                        user_data = (str(row.id), row.first_name, row.last_name, row.gender,
                                    row.address, row.post_code, row.email,
                                    row.username, row.registered_date, row.phone, row.picture)
                        insert_data(connection, user_data)
                    # delete_processed_data()
            except Error as e:
                logging.error(f"Error connecting to MySQL: {e}")
            finally:
                if connection.is_connected():
                    connection.close()
                    logging.info("MySQL connection closed.")