import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import (
    col,
    countDistinct,
    count,
    sum as _sum,
    when
)

# Initialize Glue job
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read CLEAN data from Glue Catalog
clean_df = glueContext.create_dynamic_frame.from_catalog(
    database="project_events_data_lake",
    table_name="clean_events"
).toDF()

# Curated metrics per day
curated_df = clean_df.groupBy("event_date").agg(
    countDistinct("customer_id").alias("daily_active_users"),
    count("*").alias("total_events"),
    count(when(col("event_type") == "purchase", True)).alias("total_purchases"),
    _sum(when(col("event_type") == "purchase", col("amount")).otherwise(0)).alias("total_revenue"),
    count(when(col("event_type") == "login", True)).alias("login_count"),
    count(when(col("event_type") == "purchase", True)).alias("purchase_count")
)

# Write curated data to S3
output_path = "s3://project-Vedashree-data-lake/curated/events/"
curated_df.write.mode("overwrite").parquet(output_path)

job.commit()
