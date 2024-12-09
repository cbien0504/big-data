from pyspark.sql.types import StructType, StructField, StringType, LongType, ArrayType, MapType

class TweetDocument:
    def get_schema(self):
        retweet_schema = StructType([
            StructField("_id", StringType(), True),
            StructField("author", StringType(), True),
            StructField("authorName", StringType(), True),
            StructField("created", StringType(), True),
            StructField("hashTags", ArrayType(StringType()), True),
            StructField("likes", LongType(), True),
            StructField("replyCounts", LongType(), True),
            StructField("retweetCounts", LongType(), True),
            StructField("retweetedTweet", StringType(), True),
            StructField("text", StringType(), True),
            StructField("timestamp", LongType(), True),
            StructField("url", StringType(), True),
            StructField("userMentions", MapType(StringType(), StringType(), True), True),
            StructField("views", LongType(), True)
        ])

        return StructType([
            StructField("_id", StringType(), True),
            StructField("author", StringType(), True),
            StructField("authorName", StringType(), True),
            StructField("created", StringType(), True),
            StructField("hashTags", ArrayType(StringType()), True),
            StructField("likes", LongType(), True),
            StructField("replyCounts", LongType(), True),
            StructField("retweetCounts", LongType(), True),
            StructField("retweetedTweet", retweet_schema, True),
            StructField("text", StringType(), True),
            StructField("timestamp", LongType(), True),
            StructField("url", StringType(), True),
            StructField("userMentions", MapType(StringType(), StringType(), True), True),
            StructField("views", LongType(), True)
        ])
