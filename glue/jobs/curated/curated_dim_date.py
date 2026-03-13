import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

from pyspark.sql.functions import (
    col,
    lit,
    explode,
    year,
    month,
    dayofmonth,
    dayofweek,
    quarter,
    date_format,
    when,
    expr,
)

# Glue boilerplate
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# CONFIGURATION

# Calendar range for the date dimension
START_DATE = "2018-01-01"
END_DATE   = "2025-12-31"

BUCKET_NAME = "nashville-crime-platform"
OUTPUT_PATH = f"s3://{BUCKET_NAME}/curated/dim_date/"

print(f"Building dim_date from {START_DATE} to {END_DATE}")
print(f"Output path: {OUTPUT_PATH}")


# 1) Generate calendar of all dates in range

# Create a single row with a sequence of dates, then explode to one row per day
calendar_df = (
    spark
        .sql(
            f"""
            SELECT sequence(
                to_date('{START_DATE}'),
                to_date('{END_DATE}'),
                interval 1 day
            ) AS date_seq
            """
        )
        .select(explode(col("date_seq")).alias("date"))
)

print(f"Raw calendar rows: {calendar_df.count()}")

# 2) Derive date attributes for dim_date

dim_date_df = (
    calendar_df
        .withColumn("date_key", date_format(col("date"), "yyyyMMdd").cast("int"))
        .withColumn("year", year(col("date")))
        .withColumn("quarter", quarter(col("date")))
        .withColumn("month", month(col("date")))
        .withColumn("month_name", date_format(col("date"), "MMMM"))
        .withColumn("day_of_month", dayofmonth(col("date")))
        # Spark dayofweek: 1 = Sunday, 7 = Saturday
        .withColumn("day_of_week", dayofweek(col("date")))
        .withColumn("day_of_week_name", date_format(col("date"), "EEEE"))
        .withColumn(
            "is_weekend",
            when(col("day_of_week").isin(1, 7), lit(True)).otherwise(lit(False))
        )
        .select(
            "date_key",
            "date",
            "year",
            "quarter",
            "month",
            "month_name",
            "day_of_month",
            "day_of_week",
            "day_of_week_name",
            "is_weekend",
        )
        .orderBy("date")
)

print(f"dim_date row count (after transform): {dim_date_df.count()}")

# 3) Write dim_date to curated layer in Parquet

dim_date_dyf = DynamicFrame.fromDF(dim_date_df, glueContext, "dim_date_dyf")

glueContext.write_dynamic_frame.from_options(
    frame=dim_date_dyf,
    connection_type="s3",
    connection_options={
        "path": OUTPUT_PATH,
        "partitionKeys": []  # no partitions for small dimension
    },
    format="parquet"
)

print("dim_date successfully written to curated layer.")

job.commit()