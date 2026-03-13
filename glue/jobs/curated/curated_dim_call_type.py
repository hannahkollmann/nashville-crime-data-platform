import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from pyspark.sql.functions import col, trim, upper, row_number
from pyspark.sql.window import Window

# CONFIG 
DB_NAME = "nashville_crime_lakehouse"

SRC_CALLS_TABLE = "clean_calls_for_service"

REF_TENCODE_TABLE = "curated_ref_tencode"          # or "ref_tencode"
REF_SUFFIX_TABLE  = "curated_ref_tencode_suffix"   # or "ref_tencode_suffix"

TARGET_S3_PATH = "s3://nashville-crime-platform/curated/dim_call_type/"

# Glue boilerplate
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# 1) Read source & lookup tables from Glue Catalog
calls_dyf = glueContext.create_dynamic_frame_from_catalog(
    database=DB_NAME,
    table_name=SRC_CALLS_TABLE,
    transformation_ctx="calls_dyf",
)

tencode_dyf = glueContext.create_dynamic_frame_from_catalog(
    database=DB_NAME,
    table_name=REF_TENCODE_TABLE,
    transformation_ctx="tencode_dyf",
)

suffix_dyf = glueContext.create_dynamic_frame_from_catalog(
    database=DB_NAME,
    table_name=REF_SUFFIX_TABLE,
    transformation_ctx="suffix_dyf",
)

print(f"Calls record count: {calls_dyf.count()}")
print(f"Tencode ref record count: {tencode_dyf.count()}")
print(f"Tencode suffix ref record count: {suffix_dyf.count()}")

# 2) Convert to DataFrames
calls_df = calls_dyf.toDF()
tencode_df = tencode_dyf.toDF()
suffix_df = suffix_dyf.toDF()

# 3) Build base distinct call types from calls table
#     – one row per (tencode, tencode_suffix)
base_df = (
    calls_df
        .select("tencode", "tencode_suffix")
        .where(col("tencode").isNotNull())
)

# Normalize types / casing a bit
base_df = (
    base_df
        .withColumn("tencode", col("tencode").cast("int"))
        .withColumn("tencode_suffix", trim(upper(col("tencode_suffix"))))
        .dropDuplicates(["tencode", "tencode_suffix"])
)

print(f"Distinct (tencode, suffix) combos from calls: {base_df.count()}")

# 4) Prepare reference lookups
# Tencode lookup: tencode, tencode_description
ten_ref_df = (
    tencode_df
        .select(
            col("tencode").cast("int").alias("ref_tencode"),
            trim(col("tencode_description")).alias("tencode_description"),
        )
)

# Suffix lookup: tencode_suffix, tencode_suffix_description
# (column names already cleaned up by your curated_ref_tencode_suffix job)
suf_ref_df = (
    suffix_df
        .select(
            trim(upper(col("tencode_suffix"))).alias("ref_suffix"),
            trim(col("tencode_suffix_description")).alias("tencode_suffix_description"),
        )
)

# 5) Join base with lookups
joined_df = (
    base_df
        .join(
            ten_ref_df,
            base_df["tencode"] == ten_ref_df["ref_tencode"],
            how="left",
        )
        .drop("ref_tencode")
        .join(
            suf_ref_df,
            base_df["tencode_suffix"] == suf_ref_df["ref_suffix"],
            how="left",
        )
        .drop("ref_suffix")
)

print(f"Joined rows (after lookups): {joined_df.count()}")

# 6) Add surrogate key call_type_key
w = Window.orderBy(
    col("tencode").asc_nulls_last(),
    col("tencode_suffix").asc_nulls_last(),
)

dim_df = (
    joined_df
        .withColumn("call_type_key", row_number().over(w))
        .select(
            "call_type_key",
            "tencode",
            "tencode_description",
            "tencode_suffix",
            "tencode_suffix_description",
        )
        .orderBy("call_type_key")
)

print(f"Final dim_call_type record count: {dim_df.count()}")

# 7) Back to DynamicFrame and write to curated S3 as Parquet
dim_dyf = DynamicFrame.fromDF(dim_df, glueContext, "dim_call_type_dyf")

glueContext.write_dynamic_frame.from_options(
    frame=dim_dyf,
    connection_type="s3",
    connection_options={"path": TARGET_S3_PATH},
    format="parquet",
    transformation_ctx="dim_call_type_to_s3",
)

job.commit()