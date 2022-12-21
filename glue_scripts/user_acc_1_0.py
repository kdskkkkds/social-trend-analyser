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

DataSource0 = glueContext.create_dynamic_frame.from_options(format_options = {"quoteChar":"\"","escaper":"","withHeader":True,"separator":","}, connection_type = "s3", format = "csv", connection_options = {"paths": ["s3://sta-pro-l0-prod/1.0/L0/user_account/"], "recurse":True}, transformation_ctx = "DataSource0")


Transform0 = ApplyMapping.apply(frame = DataSource0, mappings = [("id_acc", "string", "id_acc", "int"), ("user_id", "string", "user_id", "int"), ("followers_count", "string", "followers_count", "int"), ("following_count", "string", "following_count", "int"), ("private", "boolean", "private", "boolean"), ("verified", "boolean", "verified", "boolean"), ("profile_pic_url", "string", "profile_pic_url", "string"), ("no_of_posts", "string", "no_of_posts", "int"), ("blocked_in_country", "boolean", "blocked_in_country", "boolean"), ("business_account", "boolean", "business_account", "boolean"), ("audit_ts", "string", "audit_ts", "string")], transformation_ctx = "Transform0")

df = Transform0.toDF()

df = df.drop("blocked_in_country","following_count","business_account")
df = df.withColumn('date',to_date('audit_ts'))
df = df.withColumn('year',year('date')).withColumn('month',month('date')).withColumn('day',dayofmonth('date'))
df = df.drop('date')

DataSink0 = glueContext.write_dynamic_frame.from_options(frame = Transform0, connection_type = "s3", format = "glueparquet", connection_options = {"path": "s3://sta-pro-l1-prod/1.0/user_account/", "partitionKeys": ['year','month','day']}, transformation_ctx = "DataSink0")
job.commit()