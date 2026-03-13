import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql import Row
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
)

# Job Parameters & Context
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)


# Configuration
BUCKET_NAME = "nashville-crime-platform"
OUTPUT_PREFIX = "curated/dim_time/"  # s3://nashville-crime-platform/curated/dim_time/

output_path = f"s3://{BUCKET_NAME}/{OUTPUT_PREFIX}"

# Build dim_time DataFrame
# Design choice:
#   - One row per hour of day (0–23), minute fixed at 0
#   - time_key = hour * 100 + minute  (e.g., 0 -> 0, 13:00 -> 1300)
#   - time_of_day buckets:
#       Overnight: 00:00–05:59
#       Morning:   06:00–11:59
#       Afternoon: 12:00–17:59
#       Evening:   18:00–23:59

rows = []

for hour in range(24):
    minute = 0

    if 0 <= hour < 6:
        time_of_day = "Overnight"
    elif 6 <= hour < 12:
        time_of_day = "Morning"
    elif 12 <= hour < 18:
        time_of_day = "Afternoon"
    else:
        time_of_day = "Evening"

    time_key = hour * 100 + minute

    rows.append((time_key, hour, minute, time_of_day))

schema = StructType(
    [
        StructField("time_key", IntegerType(), nullable=False),
        StructField("hour", IntegerType(), nullable=False),
        StructField("minute", IntegerType(), nullable=False),
        StructField("time_of_day", StringType(), nullable=False),
    ]
)

dim_time_df = spark.createDataFrame(rows, schema)

print(f"dim_time row count: {dim_time_df.count()}")

# Convert to DynamicFrame for Glue writer
from awsglue.dynamicframe import DynamicFrame

dim_time_dyf = DynamicFrame.fromDF(dim_time_df, glueContext, "dim_time_dyf")

# Write to S3 as Parquet
glueContext.write_dynamic_frame.from_options(
    frame=dim_time_dyf,
    connection_type="s3",
    connection_options={"path": output_path},
    format="parquet",
    format_options={"compression": "snappy"},
    transformation_ctx="dim_time_write",
)

print(f"Wrote dim_time to: {output_path}")

job.commit()