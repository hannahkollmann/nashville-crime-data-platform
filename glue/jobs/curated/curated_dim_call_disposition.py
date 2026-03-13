import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import *

from pyspark.context import SparkContext
from pyspark.sql.functions import col, trim, row_number
from pyspark.sql.window import Window

# Glue boilerplate
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# CONFIG 
DB_NAME = "nashville_crime_lakehouse"
SRC_TABLE = "clean_calls_for_service"

TARGET_S3_PATH = (
    "s3://nashville-crime-platform/curated/dim_call_disposition/"
)

# 1) Read source
src_dyf = glueContext.create_dynamic_frame.from_catalog(
    database=DB_NAME,
    table_name=SRC_TABLE,
    transformation_ctx="src_calls_dyf",
)

print(f"Source rows (clean_calls_for_service): {src_dyf.count()}")

src_df = src_dyf.toDF()

# 2) Build distinct dispositions
dim_df = (
    src_df.select(
        trim(col("disposition_code")).alias("disposition_code"),
        trim(col("disposition_description")).alias(
            "disposition_description"
        ),
    )
    .filter(
        col("disposition_code").isNotNull()
        & (trim(col("disposition_code")) != "")
    )
    .dropDuplicates(["disposition_code"])
)

# Add surrogate key
w = Window.orderBy("disposition_code")
dim_df = dim_df.withColumn(
    "call_disposition_key", row_number().over(w)
)

# Reorder columns to match your star schema
dim_df = dim_df.select(
    "call_disposition_key",
    "disposition_code",
    "disposition_description",
).orderBy("call_disposition_key")

print(f"dim_call_disposition row count: {dim_df.count()}")

# 3) Back to DynamicFrame
dim_dyf = DynamicFrame.fromDF(
    dim_df, glueContext, "dim_call_disposition_dyf"
)

# 4) Write curated Parquet to S3
glueContext.write_dynamic_frame.from_options(
    frame=dim_dyf,
    connection_type="s3",
    connection_options={"path": TARGET_S3_PATH},
    format="parquet",
    transformation_ctx="dim_call_disposition_curated_s3",
)

job.commit()