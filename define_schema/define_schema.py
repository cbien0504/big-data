from pyspark.sql.types import StructType, StructField, StringType, LongType, ArrayType, MapType

class TweetDocument:
    @classmethod
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
    
class ProjectDocument:
    @classmethod
    def get_schema(self):
        return StructType([
            StructField("_id", StringType(), True),
            StructField("discord", StringType(), True),
            StructField("projectId", StringType(), True),
            StructField("telegram", StringType(), True),
            StructField("twitter", StringType(), True),
            StructField("website", StringType(), True)
        ])


class UserDocument:
    @classmethod
    def get_schema(self):
        return StructType([
            StructField("_id", StringType(), True),
            StructField("blue", StringType(), True),
            StructField("countLogs", MapType(StringType(), StructType([
                StructField("favouritesCount", LongType(), True),
                StructField("followersCount", LongType(), True),
                StructField("friendsCount", LongType(), True),
                StructField("listedCount", LongType(), True),
                StructField("mediaCount", LongType(), True),
                StructField("statusesCount", LongType(), True)
            ]), True), True),
            StructField("country", StringType(), True),
            StructField("created", StringType(), True),
            StructField("descriptionLinks", StringType(), True),
            StructField("displayName", StringType(), True),
            StructField("engagementChangeLogs", MapType(StringType(), StringType(), True), True),
            StructField("favouritesCount", LongType(), True),
            StructField("followersCount", LongType(), True),
            StructField("friendsCount", LongType(), True),
            StructField("listedCount", LongType(), True),
            StructField("location", StringType(), True),
            StructField("mediaCount", LongType(), True),
            StructField("profileBannerUrl", StringType(), True),
            StructField("profileImageUrl", StringType(), True),
            StructField("protected", StringType(), True),
            StructField("rawDescription", StringType(), True),
            StructField("statusesCount", LongType(), True),
            StructField("timestamp", LongType(), True),
            StructField("tweetCountChangeLogs", MapType(StringType(), StringType(), True), True),
            StructField("url", StringType(), True),
            StructField("userName", StringType(), True),
            StructField("verified", StringType(), True),
            StructField("viewChangeLogs", MapType(StringType(), StringType(), True), True)
        ])
class UserDocumentold:
    @classmethod
    def get_schema(self):
        return StructType([
            StructField("_id", StringType(), True),
            StructField("blue", StringType(), True),
            StructField("countLogs", MapType(StringType(), StructType([
                StructField("favouritesCount", LongType(), True),
                StructField("followersCount", LongType(), True),
                StructField("friendsCount", LongType(), True),
                StructField("listedCount", LongType(), True),
                StructField("mediaCount", LongType(), True),
                StructField("statusesCount", LongType(), True)
            ]), True), True),
            StructField("country", StringType(), True),
            StructField("created", StringType(), True),
            StructField("descriptionLinks", StringType(), True),
            StructField("displayName", StringType(), True),
            StructField("engagementChangeLogs", MapType(StringType(), StructType([
                StructField("likeCount", LongType(), True),
                StructField("replyCount", LongType(), True),
                StructField("retweetCount", LongType(), True)
            ]), True), True),
            StructField("favouritesCount", LongType(), True),
            StructField("followersCount", LongType(), True),
            StructField("friendsCount", LongType(), True),
            StructField("listedCount", LongType(), True),
            StructField("location", StringType(), True),
            StructField("mediaCount", LongType(), True),
            StructField("profileBannerUrl", StringType(), True),
            StructField("profileImageUrl", StringType(), True),
            StructField("protected", StringType(), True),
            StructField("rawDescription", StringType(), True),
            StructField("statusesCount", LongType(), True),
            StructField("timestamp", LongType(), True),
            StructField("tweetCountChangeLogs", MapType(StringType(), StringType(), True), True),
            StructField("url", StringType(), True),
            StructField("userName", StringType(), True),
            StructField("verified", StringType(), True),
            StructField("viewChangeLogs", MapType(StringType(), StringType(), True), True)
        ])