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
    date_format,
    row_number,
    trim,
)
from pyspark.sql.window import Window

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_DB = "nashville_crime_lakehouse"

CLEAN_CALLS_TABLE = "clean_calls_for_service"
DIM_LOCATION_TABLE = "dim_location"
DIM_CALL_TYPE_TABLE = "dim_call_type"
DIM_CALL_DISPOSITION_TABLE = "dim_call_disposition"

TARGET_S3_PATH = "s3://nashville-crime-platform/curated/fact_calls_for_service_v2/"

# Read source
calls_df = glueContext.create_dynamic_frame.from_catalog(
    database=SOURCE_DB,
    table_name=CLEAN_CALLS_TABLE,
    transformation_ctx="clean_calls_src",
).toDF()

dim_location_df = (
    glueContext.create_dynamic_frame.from_catalog(
        database=SOURCE_DB,
        table_name=DIM_LOCATION_TABLE,
        transformation_ctx="dim_location_src",
    )
    .toDF()
    .select("location_key", "latitude", "longitude")
    .dropDuplicates(["latitude", "longitude"])
)

dim_call_type_df = (
    glueContext.create_dynamic_frame.from_catalog(
        database=SOURCE_DB,
        table_name=DIM_CALL_TYPE_TABLE,
        transformation_ctx="dim_call_type_src",
    )
    .toDF()
    .select("call_type_key", "tencode", "tencode_suffix")
    .dropDuplicates(["tencode", "tencode_suffix"])
)

dim_call_disposition_df = (
    glueContext.create_dynamic_frame.from_catalog(
        database=SOURCE_DB,
        table_name=DIM_CALL_DISPOSITION_TABLE,
        transformation_ctx="dim_call_disposition_src",
    )
    .toDF()
    .select("call_disposition_key", "disposition_code")
    .dropDuplicates(["disposition_code"])
)

# Parse call_received: example '2025/10/28 18:10:52+00'
calls_df = (
    calls_df
    .withColumn(
        "call_received_clean",
        regexp_replace(col("call_received").cast("string"), r"\+00$", "")
    )
    .withColumn(
        "call_received_ts",
        to_timestamp(col("call_received_clean"), "yyyy/MM/dd HH:mm:ss")
    )
    .withColumn(
        "date_key",
        date_format(col("call_received_ts"), "yyyyMMdd").cast("int")
    )
    .withColumn(
        "time_key",
        date_format(col("call_received_ts"), "HH00").cast("int")
    )
)

# Validation
total_rows = calls_df.count()
null_ts_rows = calls_df.filter(col("call_received_ts").isNull()).count()
print(f"Total rows: {total_rows}")
print(f"Rows where timestamp parsing failed: {null_ts_rows}")
calls_df.select("call_received", "call_received_clean", "call_received_ts", "date_key", "time_key").show(10, truncate=False)

# Join dims
fact_df = (
    calls_df.join(
        dim_location_df,
        on=[
            calls_df.latitude == dim_location_df.latitude,
            calls_df.longitude == dim_location_df.longitude,
        ],
        how="left",
    )
    .drop(dim_location_df.latitude)
    .drop(dim_location_df.longitude)
)

fact_df = fact_df.join(
    dim_call_type_df,
    on=[
        fact_df.tencode == dim_call_type_df.tencode,
        fact_df.tencode_suffix == dim_call_type_df.tencode_suffix,
    ],
    how="left",
).drop(dim_call_type_df.tencode).drop(dim_call_type_df.tencode_suffix)

fact_df = fact_df.join(
    dim_call_disposition_df,
    on=fact_df.disposition_code == dim_call_disposition_df.disposition_code,
    how="left",
).drop(dim_call_disposition_df.disposition_code)

# Surrogate key
w = Window.orderBy(col("event_number"), col("call_received_ts"))

fact_df = (
    fact_df.withColumn("call_key", row_number().over(w).cast("int"))
    .select(
        "call_key",
        "event_number",
        "complaint_number",
        "date_key",
        "time_key",
        "location_key",
        "call_type_key",
        "call_disposition_key",
        "shift",
        "sector",
        col("call_received_ts").alias("call_received_utc"),
        "ingest_run_ts_utc",
    )
)

fact_dyf = DynamicFrame.fromDF(fact_df, glueContext, "fact_calls_dyf")

glueContext.write_dynamic_frame.from_options(
    frame=fact_dyf,
    connection_type="s3",
    connection_options={"path": TARGET_S3_PATH},
    format="parquet",
    transformation_ctx="fact_calls_to_s3",
)

job.commit()