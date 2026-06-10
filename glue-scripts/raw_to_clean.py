import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, to_date

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read raw data from Glue Catalog
raw_df = glueContext.create_dynamic_frame.from_catalog(
    database="project_events_data_lake",
    table_name="events"
).toDF()

# Add event_date column
df_with_date = raw_df.withColumn(
    "event_date",
    to_date(col("event_timestamp"))
)

# Write to clean zone in Parquet, partitioned by event_date
output_path = "s3://project-events-data-lake/clean/events/"

df_with_date.write.mode("overwrite") \
    .partitionBy("event_date") \
    .parquet(output_path)

job.commit()

#The job initializes the Glue and Spark contexts, 
#reads raw data from the Glue Catalog, 
#extracts the event date for partitioning,
#converts the data into Parquet, writes it to the clean S3 zone partitioned by date, and then commits the job.
#This improves performance, reduces Athena cost, and prepares the data for downstream curated and Redshift layers.
