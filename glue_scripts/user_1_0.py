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

DataSource0 = glueContext.create_dynamic_frame.from_options(format_options = {"quoteChar":"\"","escaper":"","withHeader":True,"separator":","}, connection_type = "s3", format = "csv", connection_options = {"paths": ["s3://sta-pro-l0-prod/1.0/L0/user/"], "recurse":True}, transformation_ctx = "DataSource0")

Transform0 = ApplyMapping.apply(frame = DataSource0, mappings = [("id_user", "string", "id_user", "int"), ("id_num", "string", "id_num", "int"), ("username", "string", "username", "string"), ("full_name", "string", "full_name", "string"), ("external_url", "string", "external_url", "string"), ("audit_ts", "string", "audit_ts", "string")], transformation_ctx = "Transform0")

df = Transform0.toDF()
df = df.drop("audit_ts")

DataSink0 = glueContext.write_dynamic_frame.from_options(frame = Transform0, format_options = {"compression": "snappy"}, connection_type = "s3", format = "glueparquet", connection_options = {"path": "s3://sta-pro-l1-prod/1.0/user/", "partitionKeys": []}, transformation_ctx = "DataSink0")
job.commit()