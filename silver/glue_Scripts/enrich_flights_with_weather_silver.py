import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import (
    col,
    broadcast,
    lit,
    radians,
    sin,
    cos,
    asin,
    sqrt,
    row_number,
    current_date,
    year,
    month
)
from pyspark.sql.window import Window

# --- Job Initialization ---

# Get job arguments
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'PLANE_SOURCE_PATH',
    'WEATHER_SOURCE_PATH',
    'DESTINATION_PATH',
    'S3_BUCKET'
])

# Initialize contexts and job
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# --- Data Reading (with Bookmark Context) ---

# Read weather data from S3 using Glue's DynamicFrame to enable bookmarking
# The transformation_ctx is a unique identifier for this read operation
weather_dyf = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [args['WEATHER_SOURCE_PATH']]},
    format="parquet",
    transformation_ctx="weather_source_bookmark"
)

# Read plane data from S3
plane_dyf = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [args['PLANE_SOURCE_PATH']]},
    format="parquet",
    transformation_ctx="plane_source_bookmark"
)

# Convert DynamicFrames to Spark DataFrames for complex transformations
df_weather = weather_dyf.toDF()
df_plane = plane_dyf.toDF()

# Exit if either dataframe is empty after reading
if df_plane.rdd.isEmpty() or df_weather.rdd.isEmpty():
    print("One of the source dataframes is empty. Exiting job.")
    job.commit()
    sys.exit(0)

# --- Data Transformation ---

# Add partition columns to the plane data for later use based on the current processing date
df_plane = df_plane.withColumn("processing_date", current_date())
df_plane = df_plane.withColumn("year", year(col("processing_date")))
df_plane = df_plane.withColumn("month", month(col("processing_date")))


# Perform a cross join with a broadcast hint for the smaller weather dataframe
# Filter the joined data to a reasonable geographic area to reduce computation
df_crossed = df_plane.crossJoin(broadcast(df_weather)) \
    .filter(
        (col("lat").between(col("latitude") - 2, col("latitude") + 2)) &
        (col("lon").between(col("longitude") - 2, col("longitude") + 2))
    )

# Define Earth's radius in kilometers
R = lit(6371)

# Convert latitude and longitude from degrees to radians for calculation
lat1 = radians(col("latitude"))
lon1 = radians(col("longitude"))
lat2 = radians(col("lat"))
lon2 = radians(col("lon"))

# Haversine formula implementation to calculate the distance between plane and weather station
dlon = lon2 - lon1
dlat = lat2 - lat1
a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
c = 2 * asin(sqrt(a))
haversine_expr = R * c

df_with_distance = df_crossed.withColumn("distance_km", haversine_expr)

# Use a window function to find the nearest weather station for each plane
window_spec = Window.partitionBy("icao24").orderBy("distance_km")
df_ranked = df_with_distance.withColumn("rank", row_number().over(window_spec))

# Filter to keep only the nearest station (rank = 1) for each plane
df_nearest = df_ranked.filter(col("rank") == 1)

# --- Data Writing (with Bookmark Context) ---

# Convert the final Spark DataFrame back to a DynamicFrame for writing
final_dyf = DynamicFrame.fromDF(df_nearest, glueContext, "final_dyf")

s3_output_path = f"s3://{args['S3_BUCKET']}/{args['DESTINATION_PATH']}"
print(f"Writing final data to {s3_output_path}...")

# Write the data to S3, partitioned by year and month
# The transformation_ctx tracks the state of this write operation for bookmarks
glueContext.write_dynamic_frame.from_options(
    frame=final_dyf,
    connection_type="s3",
    connection_options={
        "path": s3_output_path,
        "partitionKeys": ["year", "month"]
    },
    format="parquet",
    transformation_ctx="write_to_s3_bookmark"
)

# --- Job Commit ---
# Commit the job to save the state of the bookmarks
job.commit()
