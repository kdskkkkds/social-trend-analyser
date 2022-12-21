from pyspark.sql.functions import col,to_timestamp, to_date, year, month, dayofmonth
from pyspark.sql.types import DateType
import pyspark
from pyspark.sql import SparkSession
from datetime import datetime as dt

spark = SparkSession.builder.appName('account_post').getOrCreate()

spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "AKIAS5JWNKZNMNJ2NWEM")
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "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")
spark._jsc.hadoopConfiguration().set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
spark._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider")
spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "eu-west-3.amazonaws.com")

df = spark.read.options(header='True', inferSchema='True').csv("s3a://sta-pro-l0-prod/1.0/L0/account_post.csv")
df = df.drop("user_id","tagged_users")
df = df.withColumn("upload_ts",to_timestamp("upload_ts")).withColumn("performance_view_like", (df.like_count*100)/df.view_count).withColumn("performance_view_comment", (df.like_count*100)/df.view_count)
df = df.withColumn('date',to_date('upload_ts'))
df = df.withColumn('year',year('date')).withColumn('month',month('date')).withColumn('day',dayofmonth('date'))
df = df.drop('date')
df = df.drop('upload_ts')
ts = dt.now().timestamp()

df.write.partitionBy('year','month','day').parquet(f"s3://sta-pro-l1-prod/1.0/account_post_{ts}.parquet")