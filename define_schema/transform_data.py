from pyspark.sql import Row
import json

def transform_users_countLogs(spark, df):
    new_rows = []
    rows = df.collect()
    for row in rows:
        countLogs_row = row.countLogs
        if isinstance(countLogs_row, Row):
            favouritesCount_array = []
            friendsCount_array = []
            listedCount_array = []
            mediaCount_array = []
            followersCount_array = []
            statusesCount_array = []
            for key, value in countLogs_row.asDict().items():
                if value is None:
                    value = []
                elif isinstance(value, str):
                    value = json.loads(value)
                if 'favouritesCount' in value:
                    favouritesCount_array.append({ "timestamp": key, "favouritesCount": value['favouritesCount'] })
                if 'friendsCount' in value:
                    friendsCount_array.append({ "timestamp": key, "friendsCount": value['friendsCount'] })
                if 'listedCount' in value:
                    listedCount_array.append({ "timestamp": key, "listedCount": value['listedCount'] })
                if 'mediaCount' in value:
                    mediaCount_array.append({ "timestamp": key, "mediaCount": value['mediaCount'] })
                if 'followersCount' in value:
                    followersCount_array.append({ "timestamp": key, "followersCount": value['followersCount'] })
                if 'statusesCount' in value:
                    statusesCount_array.append({ "timestamp": key, "statusesCount": value['statusesCount'] })
            new_row = Row(favouritesCountLogs = favouritesCount_array,
                        friendsCountLogs = friendsCount_array,
                        listedCountLogs = listedCount_array,
                        mediaCountLogs = mediaCount_array,
                        followersCountLogs = followersCount_array,
                        statusesCountLogs = statusesCount_array,
                        **row.asDict()
                        )
            new_rows.append(new_row)
    new_df = spark.createDataFrame(new_rows)

    return new_df