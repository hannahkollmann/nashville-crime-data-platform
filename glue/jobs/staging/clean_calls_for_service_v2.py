import sys

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

from pyspark.context import SparkContext
from pyspark.sql.functions import col, regexp_replace, to_timestamp

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_DB = "nashville_crime_lakehouse"
SOURCE_TABLE = "clean_calls_for_service" 
TARGET_S3_PATH = "s3://nashville-crime-platform/clean/calls_for_service_v2/"

src_df = glueContext.create_dynamic_frame.from_catalog(
    database=SOURCE_DB,
    table_name=SOURCE_TABLE,
    transformation_ctx="calls_src",
).toDF()

clean_df = (
    src_df
    .withColumn(
        "call_received_raw",
        col("call_received").cast("string")
    )
    .withColumn(
        "call_received_clean",
        regexp_replace(col("call_received_raw"), r"\+00$", "")
    )
    .withColumn(
        "call_received",
        to_timestamp(col("call_received_clean"), "yyyy/MM/dd HH:mm:ss")
    )
)

total_rows = clean_df.count()
null_call_received = clean_df.filter(col("call_received").isNull()).count()

print(f"Total rows after parsing: {total_rows}")
print(f"Rows with NULL call_received: {null_call_received}")

clean_df.select(
    "call_received_raw",
    "call_received_clean",
    "call_received"
).show(20, truncate=False)

final_df = clean_df.drop("call_received_raw", "call_received_clean")

final_dyf = DynamicFrame.fromDF(final_df, glueContext, "clean_calls_dyf")

glueContext.write_dynamic_frame.from_options(
    frame=final_dyf,
    connection_type="s3",
    connection_options={"path": TARGET_S3_PATH},
    format="parquet",
    transformation_ctx="clean_calls_to_s3",
)

job.commit()