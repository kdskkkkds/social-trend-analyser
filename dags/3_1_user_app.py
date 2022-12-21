import pyspark
from pyspark.sql.functions import col,to_timestamp
from pyspark.sql.types import DateType
from pyspark.sql import SparkSession
from datetime import datetime as dt

spark = SparkSession.builder.appName('user').getOrCreate()

spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "AKIAS5JWNKZNMNJ2NWEM")
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "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")
spark._jsc.hadoopConfiguration().set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
spark._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider")
spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "eu-west-3.amazonaws.com")

df = spark.read.options(header='True', inferSchema='True').csv("s3a://sta-pro-l0-prod/1.0/L0/user.csv")
df = df.drop("audit_ts")
ts = dt.now().timestamp()

df.write.parquet(f"s3://sta-pro-l1-prod/1.0/user_{ts}.parquet")