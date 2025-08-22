import sys
from datetime import datetime
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import (
    explode,
    col,
    current_timestamp,
    from_utc_timestamp,
    year,
    month,
    dayofmonth,
    broadcast,
    lit,
    radians,
    sin,
    cos,
    asin,
    sqrt,
    row_number
)
from pyspark.sql.types import (
    StructType, StructField, StringType, ArrayType, LongType,
    IntegerType, FloatType, BooleanType, DoubleType
)
from pyspark.sql.window import Window

# --- 1. Initialization and Argument Parsing ---
# Retrieve job arguments passed to the script
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'PLANE_SOURCE_PATH',
    'WEATHER_SOURCE_PATH',
    'S3_BUCKET',
    'DESTINATION_PATH'
])

# Initialize Spark and Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Assign arguments to variables
PLANE_SOURCE_PATH = args['PLANE_SOURCE_PATH']
WEATHER_SOURCE_PATH = args['WEATHER_SOURCE_PATH']
S3_BUCKET = args['S3_BUCKET']
DESTINATION_PATH = args['DESTINATION_PATH']

# Validate that all necessary paths are provided
if not all([PLANE_SOURCE_PATH, WEATHER_SOURCE_PATH, S3_BUCKET, DESTINATION_PATH]):
    print('Missing Environment Variables')
    raise ValueError("All source/destination paths and the S3 bucket must be provided.")

# --- 2. Process Plane Data ---
try:
    print("Starting Plane Data Processing...")
    # Define the schema for the nested plane data JSON to optimize read performance
    state_vector_schema = ArrayType(ArrayType(StringType()))
    input_schema = StructType([
        StructField("time", LongType(), True),
        StructField("states", state_vector_schema, True)
    ])

    # Read the raw plane data from S3
    df_raw_plane = spark.read.format('json') \
        .schema(input_schema) \
        .option('multiline', True) \
        .load(PLANE_SOURCE_PATH)

    # Explode the nested 'states' array into individual rows
    df_exploded_plane = df_raw_plane.select(explode('states').alias('state_data'))
    print("Plane Data Loaded and Exploded Successfully.")

    # Define the column names and their corresponding data types for the state vector
    columns_with_types = [
        ('icao24', StringType()), ('callsign', StringType()), ('origin_country', StringType()),
        ('time_position', IntegerType()), ('last_contact', IntegerType()), ('longitude', FloatType()),
        ('latitude', FloatType()), ('baro_altitude', FloatType()), ('on_ground', BooleanType()),
        ('velocity', FloatType()), ('true_track', FloatType()), ('vertical_rate', FloatType()),
        ('sensors', StringType()), ('geo_altitude', FloatType()), ('squawk', StringType()),
        ('spi', BooleanType()), ('position_source', IntegerType())
    ]

    # Create select expressions to extract, cast, and alias each element from the state_data array
    select_exprs = [
        col('state_data').getItem(i).cast(dtype).alias(name)
        for i, (name, dtype) in enumerate(columns_with_types)
    ]

    df_transformed_plane = df_exploded_plane.select(*select_exprs)

    # Add a timestamp for when the processing occurred
    df_with_ts = df_transformed_plane.withColumn("processed_timestamp", from_utc_timestamp(current_timestamp(), "Asia/Kolkata"))

    # Add partition columns (year, month, day) for efficient data storage and querying
    df_final_plane = df_with_ts.withColumn("year", year(col("processed_timestamp"))) \
        .withColumn("month", month(col("processed_timestamp"))) \
        .withColumn("day", dayofmonth(col("processed_timestamp")))

    print("Plane Data Transformed Successfully.")

except Exception as e:
    print(f"An error occurred during plane data processing: {e}")
    job.commit()
    raise e

# --- 3. Process Weather Data ---
try:
    print("Starting Weather Data Processing...")
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

    df_raw_weather = spark.read.format('json') \
        .option('multiline', True) \
        .schema(weather_schema) \
        .load(WEATHER_SOURCE_PATH)

    df_main_weather = df_raw_weather.select(
        col("name").alias("city"),
        col("sys.country").alias("country"),
        col("coord.lat").alias("weather_lat"),
        col("coord.lon").alias("weather_lon"),
        col("weather")[0]["main"].alias("weather"),
        col("weather")[0]["description"].alias("description"),
        col("main.temp").alias("temp"),
        col("main.humidity").alias("humidity"),
        col("wind.speed").alias("wind_speed")
    )
    print("Weather Data Transformed Successfully.")

except Exception as e:
    print(f"An error occurred during weather data processing: {e}")
    job.commit()
    raise e

print("Joining plane and weather data...")
# Use a cross join with a preliminary filter to reduce the search space.
# This is more efficient than a full cross join on large datasets.
# We broadcast the smaller weather DataFrame for performance.
df_crossed = df_final_plane.crossJoin(broadcast(df_main_weather)) \
    .filter(
        (col("weather_lat").between(col("latitude") - 2, col("latitude") + 2)) &
        (col("weather_lon").between(col("longitude") - 2, col("longitude") + 2))
    )

print("Calculating Haversine distance...")
# Earth's radius in kilometers
R = lit(6371)

# Convert latitude and longitude from degrees to radians for calculation
lat1 = radians(col("latitude"))
lon1 = radians(col("longitude"))
lat2 = radians(col("weather_lat"))
lon2 = radians(col("weather_lon"))

# Haversine formula implementation
dlon = lon2 - lon1
dlat = lat2 - lat1
a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
c = 2 * asin(sqrt(a))
haversine_expr = R * c

df_with_distance = df_crossed.withColumn("distance_km", haversine_expr)

print("Finding the nearest weather station for each plane...")
# Use a window function to rank weather stations by distance for each plane
window_spec = Window.partitionBy("icao24").orderBy("distance_km")
df_ranked = df_with_distance.withColumn("rank", row_number().over(window_spec))

# Filter to keep only the nearest station (rank = 1) for each plane
df_nearest = df_ranked.filter(col("rank") == 1)

s3_output_path = f"s3://{S3_BUCKET}/{DESTINATION_PATH}"

print(f"Writing final data to {s3_output_path}...")

df_nearest.write.mode("overwrite") \
    .partitionBy("year", "month") \
    .parquet(s3_output_path)

print(f"Data successfully written to {s3_output_path}")


job.commit()
