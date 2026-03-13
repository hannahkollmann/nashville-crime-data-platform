import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Glue boilerplate
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

DATABASE = "nashville_crime_lakehouse"

# 1) Read curated/clean tables
clean_incidents_dyf = glueContext.create_dynamic_frame.from_catalog(
    database=DATABASE,
    table_name="clean_incidents",
    transformation_ctx="clean_incidents_dyf",
)

clean_calls_dyf = glueContext.create_dynamic_frame.from_catalog(
    database=DATABASE,
    table_name="clean_calls_for_service",
    transformation_ctx="clean_calls_dyf",
)

clean_incidents_df = clean_incidents_dyf.toDF()
clean_calls_df = clean_calls_dyf.toDF()

# 2) Select location columns
#    and align names

# From incidents:
# latitude, longitude, mapped_location (text), zone, rpa
inc_loc_df = (
    clean_incidents_df
    .select(
        F.col("latitude").cast("double").alias("latitude"),
        F.col("longitude").cast("double").alias("longitude"),
        F.col("mapped_location").alias("block_address"),
        F.col("zone").cast("string").alias("zone"),
        F.col("rpa").cast("string").alias("rpa"),
    )
)

# From calls for service:
# latitude, longitude, street_name (as block_address), zone_ (struct)?, rpa
# zone_ sometimes comes in as a struct; we defensively cast to string
calls_zone_col = (
    F.col("zone_").cast("string").alias("zone")
    if "zone_" in clean_calls_df.columns
    else F.lit(None).cast("string").alias("zone")
)

calls_loc_df = (
    clean_calls_df
    .select(
        F.col("latitude").cast("double").alias("latitude"),
        F.col("longitude").cast("double").alias("longitude"),
        F.col("street_name").alias("block_address"),
        calls_zone_col,
        F.col("rpa").cast("string").alias("rpa"),
    )
)

# 3) Union & deduplicate
union_loc_df = inc_loc_df.unionByName(calls_loc_df, allowMissingColumns=True)

# Drop rows with no coordinates (optional safety)
union_loc_df = union_loc_df.filter(
    (F.col("latitude").isNotNull()) & (F.col("longitude").isNotNull())
)

# Deduplicate on the natural location attributes
dedup_loc_df = union_loc_df.dropDuplicates([
    "latitude", "longitude", "block_address", "zone", "rpa"
])

# 4) Add surrogate key (location_key)
w = Window.orderBy(
    F.col("latitude"),
    F.col("longitude"),
    F.col("block_address"),
    F.col("zone"),
    F.col("rpa"),
)

dim_location_df = (
    dedup_loc_df
    .withColumn("location_key", F.row_number().over(w))
    .select(
        "location_key",
        "latitude",
        "longitude",
        "block_address",
        "zone",
        "rpa",
    )
)

print(f"dim_location row count: {dim_location_df.count()}")

# 5) Write to curated S3 (Parquet)
dim_location_dyf = DynamicFrame.fromDF(
    dim_location_df,
    glueContext,
    "dim_location_dyf"
)

target_path = "s3://nashville-crime-platform/curated/dim_location/"

glueContext.write_dynamic_frame.from_options(
    frame=dim_location_dyf,
    connection_type="s3",
    connection_options={"path": target_path},
    format="parquet",
    transformation_ctx="dim_location_s3_write",
)

job.commit()