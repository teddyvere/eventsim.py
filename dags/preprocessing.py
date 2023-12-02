import json
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as f, Row, types as t


def convert_timestamp_to_date(ts):
    obj = datetime.fromtimestamp(ts/1000)
    return datetime.strftime(obj, '%Y-%m-%d')


spark = SparkSession.Builder().appName("eventsim").getOrCreate()
df = spark.read.option("multiline", "true").json('file_path')

# Convert timestamp to date
convert_timestamp_udf = f.udf(convert_timestamp_to_date)
df = df.withColumn('date', convert_timestamp_udf(f.col("ts")))

# DAU and Session Counts
df.groupBy("date") \
    .agg(
        f.countDistinct("userId").alias("DAU"),
        f.count("sessionId").alias("Sessions")
        ).show()

# DAU by Gender
df.groupBy("date", "gender").agg(f.countDistinct("userId").alias("DAU_by_gender")).show()

# Top 10 Location with Users
df.groupBy("date", "location").agg(
    f.countDistinct("userId").alias("users_by_location")
).sort("users_by_location", ascneding=False).show(10)

# Top 10 Popular Songs
df.groupBy("date", "artist", "song").count().sort("count", ascending=False).show(10)