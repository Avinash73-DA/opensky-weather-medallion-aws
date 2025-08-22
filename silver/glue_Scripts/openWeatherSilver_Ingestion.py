import sys
from datetime import datetime
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import col, from_utc_timestamp, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, ArrayType

# --- Job Initialization ---
args = getResolvedOptions(sys.argv,['JOB_NAME','SOURCE_PATH','S3_BUCKET','DESTINATION_PATH'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

SOURCE_PATH = args['SOURCE_PATH']
S3_BUCKET = args['S3_BUCKET']
DESTINATION_PATH = args['DESTINATION_PATH']

if not all([SOURCE_PATH, S3_BUCKET, DESTINATION_PATH]):
    print("Missing Environment Variables")
    raise ValueError("SOURCE_PATH, S3_BUCKET, and DESTINATION_PATH must be provided.")

# --- Schema Definition ---
weather_schema = StructType([
    StructField("coord", StructType([
        StructField("lon", DoubleType(), True),
        StructField("lat", DoubleType(), True)
    ]), True),
    StructField("weather", ArrayType(StructType([
        StructField("main", StringType(), True),
        StructField("description", StringType(), True)
    ])), True),
    StructField("main", StructType([
        StructField("temp", DoubleType(), True),
        StructField("humidity", LongType(), True)
    ]), True),
    StructField("wind", StructType([
        StructField("speed", DoubleType(), True)
    ]), True),
    StructField("sys", StructType([
        StructField("country", StringType(), True)
    ]), True),
    StructField("name", StringType(), True)
])

df_raw = spark.read.format('json')\
                  .option('multiline',True)\
                  .schema(weather_schema)\
                  .load(SOURCE_PATH)

# --- Transformations (No changes needed) ---
df_main_weather = df_raw.select(
    col("name").alias("city"),
    col("sys.country").alias("country"),
    col("coord.lat").alias("lat"),
    col("coord.lon").alias("lon"),
    col("weather")[0]["main"].alias("weather"),
    col("weather")[0]["description"].alias("description"),
    col("main.temp").alias("temp"),
    col("main.humidity").alias("humidity"),
    col("wind.speed").alias("wind_speed"),
    from_utc_timestamp(current_timestamp(), "Asia/Kolkata").alias("timestamp_sil")
)

s3_output_path = f"s3://{S3_BUCKET}/{DESTINATION_PATH}"

# --- Write Data
df_main_weather.coalesce(1).write.mode("overwrite")\
    .partitionBy('country')\
    .parquet(s3_output_path) 

print(f"Data Succesfully written to {s3_output_path}")

job.commit()