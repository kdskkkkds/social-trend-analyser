import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col,to_timestamp, to_date, year, month, dayofmonth

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

DataSource0 = glueContext.create_dynamic_frame.from_options(format_options = {"quoteChar":"\"","escaper":"","withHeader":True,"separator":","}, connection_type = "s3", format = "csv", connection_options = {"paths": ["s3://sta-pro-l0-prod/1.0/L0/account_post/"], "recurse":True}, transformation_ctx = "DataSource0")


Transform0 = ApplyMapping.apply(frame = DataSource0, mappings = [("id_post", "string", "id_post", "int"), ("user_id", "string", "user_id", "int"), ("post_unique_code", "string", "post_unique_code", "string"), ("post_url", "string", "post_url", "string"), ("video", "boolean", "video", "boolean"), ("audio_inclusion", "boolean", "audio_inclusion", "boolean"), ("tagged_users", "string", "tagged_users", "string"), ("view_count", "string", "view_count", "int"), ("comment_count", "string", "comment_count", "int"), ("like_count", "string", "like_count", "int"), ("hashtag", "string", "hashtag", "string"), ("upload_ts", "string", "upload_ts", "int"), ("audit_ts", "string", "audit_ts", "string")], transformation_ctx = "Transform0")

df = Transform0.toDF()
df = df.drop("user_id","tagged_users")
df = df.withColumn("upload_ts",to_timestamp("upload_ts")).withColumn("performance_view_like", (df.like_count*100)/df.view_count).withColumn("performance_view_comment", (df.like_count*100)/df.view_count)
df = df.withColumn('date',to_date('upload_ts'))
df = df.withColumn('year',year('date')).withColumn('month',month('date')).withColumn('day',dayofmonth('date'))
df = df.drop('date')
df = df.drop('upload_ts')

DataSink0 = glueContext.write_dynamic_frame.from_options(frame = Transform0, format_options = {"compression": "snappy"}, connection_type = "s3", format = "glueparquet", connection_options = {"path": "s3://sta-pro-l1-prod/1.0/account_post/", "partitionKeys": ['year','month','day']}, transformation_ctx = "DataSink0")
job.commit()