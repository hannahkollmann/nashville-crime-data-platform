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
SOURCE_TABLE = "clean_incidents_v2"
TARGET_S3_PATH = "s3://nashville-crime-platform/curated/fact_incidents_v2/"

# Read cleaned incidents
src_df = glueContext.create_dynamic_frame.from_catalog(
    database=SOURCE_DB,
    table_name=SOURCE_TABLE
).toDF()

# Generate surrogate date and time keys
fact_df = (
    src_df
    .withColumn(
        "date_key",
        date_format(col("incident_occurred"), "yyyyMMdd").cast("int")
    )
    .withColumn(
        "time_key",
        date_format(col("incident_occurred"), "HH00").cast("int")
    )
)

# Create incident surrogate key
window = Window.orderBy(col("incident_number"))

fact_df = fact_df.withColumn(
    "incident_key",
    row_number().over(window).cast("int")
)

fact_df = fact_df.select(
    "incident_key",
    "incident_number",
    "date_key",
    "time_key",
    "offense_number",
    "offense_nibrs",
    "latitude",
    "longitude",
    "incident_occurred",
    "incident_reported"
)

fact_dyf = DynamicFrame.fromDF(fact_df, glueContext, "fact_incidents_dyf")

glueContext.write_dynamic_frame.from_options(
    frame=fact_dyf,
    connection_type="s3",
    connection_options={"path": TARGET_S3_PATH},
    format="parquet"
)

job.commit()