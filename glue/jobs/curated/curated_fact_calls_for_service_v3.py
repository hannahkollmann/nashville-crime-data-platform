import sys

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

from pyspark.context import SparkContext
from pyspark.sql.functions import col, date_format, row_number
from pyspark.sql.window import Window

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_DB = "nashville_crime_lakehouse"
SOURCE_TABLE = "clean_calls_for_service_v2"
TARGET_PATH = "s3://nashville-crime-platform/curated/fact_calls_for_service_v3/"

src_df = glueContext.create_dynamic_frame.from_catalog(
    database=SOURCE_DB,
    table_name=SOURCE_TABLE
).toDF()

# Create surrogate keys
fact_df = (
    src_df
    .withColumn(
        "date_key",
        date_format(col("call_received"), "yyyyMMdd").cast("int")
    )
    .withColumn(
        "time_key",
        date_format(col("call_received"), "HH00").cast("int")
    )
)

# Generate primary key
window = Window.orderBy(col("complaint_number"))

fact_df = fact_df.withColumn(
    "call_key",
    row_number().over(window)
)

fact_df = fact_df.select(
    "call_key",
    "complaint_number",
    "date_key",
    "time_key",
    "latitude",
    "longitude",
    "rpa",
    col("zone_").alias("zone"),
    "call_received",
    "ingest_run_ts_utc"
)

fact_dyf = DynamicFrame.fromDF(fact_df, glueContext, "fact_calls")

glueContext.write_dynamic_frame.from_options(
    frame=fact_dyf,
    connection_type="s3",
    connection_options={"path": TARGET_PATH},
    format="parquet"
)

job.commit()