import sys

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

from pyspark.context import SparkContext
from pyspark.sql.functions import (
    col,
    regexp_replace,
    to_timestamp,
)

# Job setup
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Config
SOURCE_DB = "nashville_crime_lakehouse"
SOURCE_TABLE = "raw_incidents"
TARGET_S3_PATH = "s3://nashville-crime-platform/clean/incidents_v2/"

# 1) Read source table from Glue Catalog
src_dyf = glueContext.create_dynamic_frame.from_catalog(
    database=SOURCE_DB,
    table_name=SOURCE_TABLE,
    transformation_ctx="raw_incidents_src",
)

src_df = src_dyf.toDF()

print(f"Source row count: {src_df.count()}")

# 2) Parse incident_occurred
# Raw format is expected like:
#   2025/10/28 18:10:52+00
# We strip '+00' and parse yyyy/MM/dd HH:mm:ss
clean_df = (
    src_df
    .withColumn(
        "incident_occurred_raw",
        col("incident_occurred").cast("string")
    )
    .withColumn(
        "incident_occurred_clean",
        regexp_replace(col("incident_occurred_raw"), r"\+00$", "")
    )
    .withColumn(
        "incident_occurred",
        to_timestamp(col("incident_occurred_clean"), "yyyy/MM/dd HH:mm:ss")
    )
)

# 3) Parse incident_reported
clean_df = (
    clean_df
    .withColumn(
        "incident_reported_raw",
        col("incident_reported").cast("string")
    )
    .withColumn(
        "incident_reported_clean",
        regexp_replace(col("incident_reported_raw"), r"\+00$", "")
    )
    .withColumn(
        "incident_reported",
        to_timestamp(col("incident_reported_clean"), "yyyy/MM/dd HH:mm:ss")
    )
)

# 4) Validation prints
total_rows = clean_df.count()
null_occurred = clean_df.filter(col("incident_occurred").isNull()).count()
null_reported = clean_df.filter(col("incident_reported").isNull()).count()

print(f"Total rows after parsing: {total_rows}")
print(f"Rows with NULL incident_occurred: {null_occurred}")
print(f"Rows with NULL incident_reported: {null_reported}")

print("Sample parsed timestamp rows:")
clean_df.select(
    "incident_occurred_raw",
    "incident_occurred_clean",
    "incident_occurred",
    "incident_reported_raw",
    "incident_reported_clean",
    "incident_reported",
).show(20, truncate=False)

# 5) Drop helper columns before writing
final_df = clean_df.drop(
    "incident_occurred_raw",
    "incident_occurred_clean",
    "incident_reported_raw",
    "incident_reported_clean",
)

# 6) Convert back to DynamicFrame and write to S3
final_dyf = DynamicFrame.fromDF(final_df, glueContext, "clean_incidents_dyf")

glueContext.write_dynamic_frame.from_options(
    frame=final_dyf,
    connection_type="s3",
    connection_options={"path": TARGET_S3_PATH},
    format="parquet",
    transformation_ctx="clean_incidents_to_s3",
)

job.commit()