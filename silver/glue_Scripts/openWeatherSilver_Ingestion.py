import sys
from datetime import datetime
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from awsglue.transforms import ResolveChoice
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, from_utc_timestamp, current_timestamp, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, ArrayType

# --- Job Initialization ---
args = getResolvedOptions(sys.argv, ['JOB_NAME','SOURCE_PATH','S3_BUCKET','DESTINATION_PATH'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

SOURCE_PATH = args['SOURCE_PATH']
S3_BUCKET = args['S3_BUCKET']
DESTINATION_PATH = args['DESTINATION_PATH']

if not all([SOURCE_PATH, S3_BUCKET, DESTINATION_PATH]):
    raise ValueError("SOURCE_PATH, S3_BUCKET, and DESTINATION_PATH must be provided.")

# --- Read Data using the CORRECT method for Bookmarking ---
dynamic_frame_raw = glueContext.create_dynamic_frame.from_options(
    format="json",
    connection_type="s3",
    format_options={"multiline": True},
    connection_options={
        "paths": [SOURCE_PATH],
        "recurse": True
    },
    transformation_ctx="dynamic_frame_raw"
)


resolved_frame = ResolveChoice.apply(
    frame=dynamic_frame_raw,
    specs=[
        ('main', 'cast:string'),
        ('coord.lat', 'cast:double'),
        ('coord.lon', 'cast:double'),
        ('wind.speed', 'cast:double')
    ]
)
df_raw = resolved_frame.toDF()

main_schema = StructType([
    StructField("temp", DoubleType(), True),
    StructField("humidity", LongType(), True)
])

df_parsed = df_raw.withColumn("main_parsed", from_json(col("main"), main_schema))


df_main_weather = df_parsed.filter(col("main_parsed").isNotNull()).select(
    col("name").alias("city"),
    col("sys.country").alias("country"),
    col("coord.lat").alias("lat"),
    col("coord.lon").alias("lon"),
    col("weather")[0]["main"].alias("weather"),
    col("weather")[0]["description"].alias("description"),
    col("main_parsed.temp").alias("temp"),
    col("main_parsed.humidity").alias("humidity"),
    col("wind.speed").alias("wind_speed"),
    from_utc_timestamp(current_timestamp(), "Asia/Kolkata").alias("timestamp_sil")
)

# --- Deduplication ---
df_dedup = df_main_weather.dropDuplicates(
    ["city", "country", "weather", "timestamp_sil"]
)

# --- Debugging Output ---
print("Schema of the final DataFrame:")
df_dedup.printSchema()
print("Sample of the final data (first 10 rows):")
df_dedup.show(10, truncate=False)

# --- Write Output ---
s3_output_path = f"s3://{S3_BUCKET}/{DESTINATION_PATH}"

dyf_dedup = DynamicFrame.fromDF(df_dedup, glueContext, "dyf_dedup")

glueContext.write_dynamic_frame.from_options(
    frame=dyf_dedup,
    connection_type="s3",
    connection_options={
        "path": s3_output_path,
        "partitionKeys": ["country"]
    },
    format="parquet",
    transformation_ctx="datasink"
)

print(f"Deduplicated data written to {s3_output_path}")

job.commit()