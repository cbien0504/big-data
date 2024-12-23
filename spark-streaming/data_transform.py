from pyspark.sql import Row
import json

def transform_users_countLogs(spark, df):
    new_rows = []
    rows = df.collect()
    for row in rows:
        countLogs_row = row.countLogs
        favouritesCount_dict = {}
        friendsCount_dict = {}
        listedCount_dict = {}
        mediaCount_dict = {}
        followersCount_dict = {}
        statusesCount_dict = {}
        for key, value in countLogs_row.asDict().items():
            if value is None:
                value = {}
            elif isinstance(value, str):
                value = json.loads(value)
            key = int(key)
            if 'favouritesCount' in value:
                favouritesCount_dict[key] = value['favouritesCount']
            if 'friendsCount' in value:
                friendsCount_dict[key] = value['friendsCount']
            if 'listedCount' in value:
                listedCount_dict[key] = value['listedCount']
            if 'mediaCount' in value:
                mediaCount_dict[key] = value['mediaCount']
            if 'followersCount' in value:
                followersCount_dict[key] = value['followersCount']
            if 'statusesCount' in value:
                statusesCount_dict[key] = value['statusesCount']
        new_row = Row(**row.asDict(),
                    favouritesCountLogs = favouritesCount_dict,
                    friendsCountLogs = friendsCount_dict,
                    listedCountLogs = listedCount_dict,
                    mediaCountLogs = mediaCount_dict,
                    followersCountLogs = followersCount_dict,
                    statusesCountLogs = statusesCount_dict
                    )
        new_rows.append(new_row)
    new_df = spark.createDataFrame(new_rows)
    new_df.drop("countLogs")
    return new_df