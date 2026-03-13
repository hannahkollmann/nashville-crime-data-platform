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

# 1) Read curated incidents
clean_incidents_dyf = glueContext.create_dynamic_frame.from_catalog(
    database=DATABASE,
    table_name="clean_incidents",
    transformation_ctx="clean_incidents_dyf",
)

clean_incidents_df = clean_incidents_dyf.toDF()

# 2) Select offense columns
offense_df = (
    clean_incidents_df
    .select(
        F.col("offense_number").cast("bigint").alias("offense_number"),
        F.col("offense_nibrs").cast("string").alias("offense_nibrs"),
        F.col("offense_description").cast("string").alias("offense_description"),
        F.col("weapon_primary").cast("string").alias("weapon_primary"),
    )
)

# Drop rows that don't have an offense_number or description
offense_df = offense_df.filter(
    (F.col("offense_number").isNotNull()) &
    (F.col("offense_description").isNotNull())
)

# 3) Derive offense_category & is_violent
#    (simple rule-based mapping)
desc = F.upper(F.col("offense_description"))

offense_df = offense_df.withColumn(
    "offense_category",
    F.when(
        desc.rlike("HOMICIDE|MURDER|MANSLAUGHTER|KIDNAP|KIDNAPPING"),
        F.lit("Violent")
    ).when(
        desc.rlike("ASSAULT|AGG ASSAULT|DOMESTIC ASSAULT"),
        F.lit("Violent")
    ).when(
        desc.rlike("ROBBERY|CARJACKING"),
        F.lit("Violent")
    ).when(
        desc.rlike("RAPE|SEXUAL|SODOMY|FONDLING"),
        F.lit("Violent")
    ).when(
        desc.rlike("BURGLARY|BREAKING AND ENTERING"),
        F.lit("Property")
    ).when(
        desc.rlike("THEFT|LARCENY|SHOPLIFT|STOLEN PROPERTY"),
        F.lit("Property")
    ).when(
        desc.rlike("VANDALISM|DAMAGE PROP|GRAFFITI"),
        F.lit("Property")
    ).otherwise(F.lit("Other"))
)

offense_df = offense_df.withColumn(
    "is_violent",
    F.when(F.col("offense_category") == "Violent", F.lit(True)).otherwise(F.lit(False))
)

# 4. Deduplicate at offense-type level
#    (ignore weapon differences for the key)
# Group by the logical offense attributes and aggregate weapons into a list
offense_grouped_df = (
    offense_df
    .groupBy(
        "offense_number",
        "offense_nibrs",
        "offense_description",
        "offense_category",
        "is_violent",
    )
    .agg(
        F.collect_set("weapon_primary").alias("weapon_list")
    )
)

# Turn the weapon list into a readable string (optional)
offense_grouped_df = offense_grouped_df.withColumn(
    "weapon_primary",
    F.array_join(F.col("weapon_list"), ", ")
).drop("weapon_list")

# 5) Add surrogate key (offense_key)
w = Window.orderBy(
    F.col("offense_number"),
    F.col("offense_nibrs"),
    F.col("offense_description")
)

dim_offense_df = (
    offense_grouped_df
    .withColumn("offense_key", F.row_number().over(w))
    .select(
        "offense_key",
        "offense_number",
        "offense_nibrs",
        "offense_description",
        "weapon_primary",      # aggregated list, or drop this if you don't want it
        "offense_category",
        "is_violent",
    )
)

# 6) Write to curated S3 (Parquet)
dim_offense_dyf = DynamicFrame.fromDF(
    dim_offense_df,
    glueContext,
    "dim_offense_dyf"
)

target_path = "s3://nashville-crime-platform/curated/dim_offense/"

glueContext.write_dynamic_frame.from_options(
    frame=dim_offense_dyf,
    connection_type="s3",
    connection_options={"path": target_path},
    format="parquet",
    transformation_ctx="dim_offense_s3_write",
)

job.commit()