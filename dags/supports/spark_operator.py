from dateimte import datetime

import pandas as pd
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, functions as f


spark = SparkSession(SparkContext(conf=SparkConf())).getOrCreate()

df = spark.read.format("csv")\
          .option("header", "true")\
          .option("inferSchema", "true")\
          .csv("s3a://eventsim/eventsim/date_id=2023-12-04-test.csv")

# contert timestamp to datetime
df.withColumn("ts", f.to_timestamp(f.col("ts")))\
  .withColumn("date_id", f.to_date(f.col("ts")))

grouped_df = df.groupBy("date_id")

def put_by_date_id(date_id, table):
    key = f"s3a://eventsim/silver1/date_id={date_id}/test0.csv"
    table.write.mode("overwrite").parquet(key)

grouped_df.foreach(lambda x: put_by_date_id(x[0], x[1]))