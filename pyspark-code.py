"""
AWS Glue ETL Job
Goal:
- Read the 'raw_statistics' table from the Glue Data Catalog (db_youtube_raw)
- Filter to only specific regions (ca, gb, us) using predicate pushdown
- Apply an explicit schema mapping (column names + data types)
- Clean up ambiguous types and null-only fields
- Write the output to S3 in Parquet format, partitioned by region
- Keep the same inputs and outputs as the original script
"""

# -----------------------------
# Standard library imports
# -----------------------------
import sys
# sys is used because Glue passes job parameters (like JOB_NAME) through command-line arguments (sys.argv)

# -----------------------------
# AWS Glue imports (ETL utilities)
# -----------------------------
from awsglue.transforms import *
# awsglue.transforms provides built-in Glue transforms like ApplyMapping, ResolveChoice, DropNullFields

from awsglue.utils import getResolvedOptions
# getResolvedOptions reads job arguments passed to the Glue job (e.g., --JOB_NAME)

from awsglue.dynamicframe import DynamicFrame
# DynamicFrame is Glue's native data structure (similar to a Spark DataFrame, but designed for semi-structured data)

from awsglue.context import GlueContext
# GlueContext wraps SparkContext and provides Glue-specific methods (read from catalog, write to S3, etc.)

from awsglue.job import Job
# Job lets Glue track job lifecycle (init + commit). commit() is important for bookmarks/metrics.

# -----------------------------
# Spark import (execution engine)
# -----------------------------
from pyspark.context import SparkContext
# SparkContext is the entry point for Spark. Glue runs on Spark under the hood.

# -----------------------------
# 1) Read job parameters
# -----------------------------
# Glue jobs typically receive parameters like:
# --JOB_NAME <name>
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# -----------------------------
# 2) Create Spark + Glue contexts
# -----------------------------
sc = SparkContext()                 # Starts the Spark engine
glueContext = GlueContext(sc)       # Adds Glue features on top of Spark
spark = glueContext.spark_session   # Spark session for DataFrame operations (SQL, coalesce, etc.)

# -----------------------------
# 3) Initialize Glue job (required)
# -----------------------------
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# -----------------------------
# 4) Read source data from Glue Data Catalog (input)
# -----------------------------
# Predicate pushdown means the filter is applied at read time.
# This reduces how much data is scanned/loaded, improving cost and performance.
predicate_pushdown = "region in ('ca','gb','us')"

# Read the table "raw_statistics" from database "db_youtube_raw"
# Input remains the same as the original script.
datasource0 = glueContext.create_dynamic_frame.from_catalog(
    database="db_youtube_raw",
    table_name="raw_statistics",
    transformation_ctx="datasource0",
    push_down_predicate=predicate_pushdown
)

# -----------------------------
# 5) Apply schema mapping (standardize columns + types)
# -----------------------------
# ApplyMapping is used to:
# - explicitly define the expected schema
# - enforce consistent column names and data types
# This is especially useful when raw data has inconsistent or evolving schemas.
applymapping1 = ApplyMapping.apply(
    frame=datasource0,
    mappings=[
        ("video_id", "string", "video_id", "string"),
        ("trending_date", "string", "trending_date", "string"),
        ("title", "string", "title", "string"),
        ("channel_title", "string", "channel_title", "string"),
        ("category_id", "long", "category_id", "long"),
        ("publish_time", "string", "publish_time", "string"),
        ("tags", "string", "tags", "string"),
        ("views", "long", "views", "long"),
        ("likes", "long", "likes", "long"),
        ("dislikes", "long", "dislikes", "long"),
        ("comment_count", "long", "comment_count", "long"),
