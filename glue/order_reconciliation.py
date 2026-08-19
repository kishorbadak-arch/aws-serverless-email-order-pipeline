import sys
import logging
import boto3

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, to_date, coalesce, lit

logger = logging.getLogger()
logger.setLevel(logging.INFO)

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# NOTE: we deliberately don't do spark.read.csv("s3://bucket/csv/") on a
# folder path here. Spark's directory reader only picks up files sitting
# DIRECTLY inside that folder by default — it does NOT recurse into nested
# subfolders (like csv/2026/08/13/file.csv) unless you set
# .option("recursiveFileLookup", "true"). Since the parser Lambda nests
# files under date folders, we list files explicitly with boto3 instead —
# no ambiguity about what gets read, regardless of nesting depth.

SILVER_BUCKET = "silver-data-pipline"
GOLD = "s3://gold-layer-pipline/orders/"

s3 = boto3.client("s3")

schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product", StringType(), True),
    StructField("category", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("order_date", StringType(), True),
    StructField("status", StringType(), True),
])


def get_s3_files(prefix, extension):
    files = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=SILVER_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.lower().endswith(extension):
                files.append(f"s3://{SILVER_BUCKET}/{key}")
    return files


csv_files = get_s3_files("silver/csv/", ".csv")
logger.info(f"CSV files found: {len(csv_files)}")
if csv_files:
    csv_df = spark.read.option("header", "true").schema(schema).csv(csv_files)
else:
    logger.info("No CSV files found")
    csv_df = spark.createDataFrame([], schema)

json_files = get_s3_files("silver/json/", ".json")
logger.info(f"JSON files found: {len(json_files)}")
if json_files:
    json_df = spark.read.schema(schema).json(json_files)
else:
    logger.info("No JSON files found")
    json_df = spark.createDataFrame([], schema)

parquet_files = get_s3_files("silver/parquet/", ".parquet")
logger.info(f"Parquet files found: {len(parquet_files)}")
if parquet_files:
    parquet_df = spark.read.schema(schema).parquet(parquet_files)
else:
    logger.info("No Parquet files found")
    parquet_df = spark.createDataFrame([], schema)

df = csv_df.unionByName(json_df, allowMissingColumns=True) \
            .unionByName(parquet_df, allowMissingColumns=True)

record_count = df.count()
logger.info(f"Total records read from Silver: {record_count}")

# no sys.exit() — Glue's driver flags any explicit process exit as
# SYSTEM_EXIT_ERROR and marks the whole run FAILED, even sys.exit(0).
if record_count == 0:
    logger.info("No records found in Silver. Nothing to write.")
    job.commit()
else:
    df = df.withColumn(
        "order_date_clean",
        coalesce(
            to_date(col("order_date"), "yyyy-MM-dd"),
            to_date(col("order_date"), "dd/MM/yyyy"),
            to_date(col("order_date"), "MM/dd/yyyy"),
        ),
    )
    df = df.withColumn(
        "order_date_clean",
        coalesce(col("order_date_clean"), lit("1970-01-01").cast("date")),
    )

    df = df.fillna({"category": "Unknown", "status": "Unknown"})
    df = df.dropna(subset=["order_id", "product"])
    df = df.dropDuplicates(["order_id"])

    final_count = df.count()
    logger.info(f"Records after cleaning: {final_count}")

    df.write.mode("append").partitionBy("order_date_clean").parquet(GOLD)
    logger.info(f"Successfully wrote {final_count} records to: {GOLD}")

    job.commit()
