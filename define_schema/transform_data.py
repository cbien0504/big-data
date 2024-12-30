from pyspark.sql import Row
from pyspark.sql import functions as F
from datetime import datetime, timezone, timedelta
import json
from sklearn.linear_model import LinearRegression
import numpy as np
import pandas as pd

def transform_users_countLogs(spark, df, predict=False):
    df = df.select("userName", "countLogs")
    new_rows = []
    rows = df.collect()

    for row in rows:
        countLogs_row = row.countLogs
        if isinstance(countLogs_row, Row):
            timestamp_array, count_array = [] , []
            for key, value in countLogs_row.asDict().items():
                iso_time = datetime.fromtimestamp(int(key), tz=timezone.utc).isoformat()
                if value is None:
                    value = []
                elif isinstance(value, str):
                    value = json.loads(value)
                if 'followersCount' in value:
                    timestamp_array.append(iso_time)
                    count_array.append(value['followersCount'])
                    new_row = Row(
                        Timestamp=iso_time,
                        followersCountLogs=value['followersCount'],
                        **{k: v for k, v in row.asDict().items() if k != 'countLogs'}
                    )
                    new_rows.append(new_row)
            if predict:
                data = pd.DataFrame({
                    "timestamp": pd.to_datetime(timestamp_array),
                    "value": count_array
                })
                data["epoch"] = data["timestamp"].apply(lambda x: int(x.timestamp()))
                X = data["epoch"].values.reshape(-1, 1)  # Feature (thời gian)
                y = data["value"].values                # Label (giá trị)
                model = LinearRegression()
                model.fit(X, y)
                last_epoch = data["epoch"].max()
                future_epochs = [last_epoch + i * 24 * 60 * 60 for i in range(1, 31)]
                future_timestamps = [
                    datetime.fromtimestamp(e, tz=timezone.utc).replace(hour=0, minute=0, second=0).isoformat()
                    for e in future_epochs
                ]
                future_X = np.array(future_epochs).reshape(-1, 1)
                predicted_values = model.predict(future_X)
                future_data = pd.DataFrame({
                "timestamp": future_timestamps,
                "predicted_value": predicted_values.astype(int)
                })
                for index, pdrow in future_data.iterrows():
                    timestamp = pdrow['timestamp']
                    value = pdrow['predicted_value']
                    new_row = Row(
                        Timestamp=timestamp,
                        followersCountLogs=value,
                        **{k: v for k, v in row.asDict().items() if k != 'countLogs'}
                    )
                    new_rows.append(new_row)
    new_df = spark.createDataFrame(new_rows)
    return new_df
