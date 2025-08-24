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
    dayofmonth
)
from pyspark.sql.types import (
    StringType, ArrayType, LongType,
    IntegerType, FloatType, BooleanType
)

# --- 1. Initialization ---
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'SOURCE_PATH', 'BUCKET'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

SOURCE_PATH = args['SOURCE_PATH']
BUCKET = args['BUCKET']

if not all([SOURCE_PATH, BUCKET]):
    print('Missing Environment Variables')
    raise ValueError("SOURCE_PATH and BUCKET must be provided.")

try:
    # --- 2. Read with Glue DynamicFrame to enable Bookmark ---
    datasource = glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={
            "paths": [SOURCE_PATH],
            "recurse": True,
        },
        format="json",
        transformation_ctx="datasource" # This name is key for the bookmark
    )

    df_raw = datasource.toDF()
    
    # Check if the dataframe is empty after bookmarking; if so, exit gracefully.
    if df_raw.rdd.isEmpty():
        print("No new data to process. Committing bookmark and exiting.")
        job.commit()
        sys.exit(0)

    exploded_view = df_raw.select(explode('states').alias('state_data'))
    print("Data Loaded Successfully with Bookmark Enabled")

    # --- 3. Fully Robust Transformation Logic ---
    columns_with_types = [
        ('icao24', StringType()), ('callsign', StringType()), ('origin_country', StringType()),
        ('time_position', IntegerType()), ('last_contact', IntegerType()), ('longitude', FloatType()),
        ('latitude', FloatType()), ('baro_altitude', FloatType()), ('on_ground', BooleanType()),
        ('velocity', FloatType()), ('true_track', FloatType()), ('vertical_rate', FloatType()),
        ('sensors', StringType()), ('geo_altitude', FloatType()), ('squawk', StringType()),
        ('spi', BooleanType()), ('position_source', IntegerType())
    ]

    # ✅ FINAL FIX: Proactively handle all fields that might be nested inside a struct.
    # This comprehensive approach prevents future errors by assuming any numeric or
    # boolean field could come in the inconsistent nested format.
    select_exprs = []
    
    # Define all fields that might be nested structs based on their target data type
    int_struct_fields = ['time_position', 'last_contact', 'position_source']
    float_struct_fields = [
        'longitude', 'latitude', 'baro_altitude', 'velocity', 
        'true_track', 'vertical_rate', 'geo_altitude'
    ] 
    bool_struct_fields = ['on_ground', 'spi']

    for i, (name, dtype) in enumerate(columns_with_types):
        if name in int_struct_fields:
            # For integer fields, access the nested 'int' value from the struct.
            expr = col('state_data').getItem(i).getField('int').cast(dtype).alias(name)
        elif name in float_struct_fields:
            # For float fields, access the nested 'double' value from the struct.
            expr = col('state_data').getItem(i).getField('double').cast(dtype).alias(name)
        elif name in bool_struct_fields:
            # For boolean fields, access the nested 'boolean' value from the struct.
            expr = col('state_data').getItem(i).getField('boolean').cast(dtype).alias(name)
        else:
            # For string and other consistent fields, use direct casting.
            expr = col('state_data').getItem(i).cast(dtype).alias(name)
        select_exprs.append(expr)
    
    df_transformed = exploded_view.select(*select_exprs)

    # --- 4. Add Partition Columns ---
    df_with_ts = df_transformed.withColumn(
        "processed_timestamp",
        from_utc_timestamp(current_timestamp(), "Asia/Kolkata")
    )

    df_final = df_with_ts.withColumn("year", year(col("processed_timestamp"))) \
                         .withColumn("month", month(col("processed_timestamp"))) \
                         .withColumn("day", dayofmonth(col("processed_timestamp")))
    
    print("Data Transformed Successfully")

    s3_output_path = f"s3://{BUCKET}/silver/planes_data/" 
    
    # Write data with year, month, and day partitioning
    df_final.write.mode("append") \
            .partitionBy("year", "month", "day") \
            .parquet(s3_output_path)
            
    print(f"Data Written Successfully to {s3_output_path}")

except Exception as e:
    print(f"An error occurred: {e}")
    # Do NOT commit the job on failure. Let it fail so it can be retried.
    raise e

# --- 5. Commit the job ONLY on successful completion ---
# This saves the bookmark state for the next run.
job.commit()
