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
    udf
)
from pyspark.sql.types import (
    StringType, ArrayType, LongType,
    IntegerType, FloatType, BooleanType,
    StructType, StructField
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

    # --- 3. Simplified Transformation Logic using a UDF ---

    # This UDF handles the inconsistent data structure. It checks if the data
    # is a simple value or a nested object (struct) and extracts the correct
    # value, returning it as a string.
    def extract_value_from_item(item):
        # If the item is None, return None
        if item is None:
            return None
        # Check if the item is a struct-like object (a Row in PySpark)
        if hasattr(item, '__fields__'):
            # Check for possible nested values in order of likelihood
            if 'string' in item.__fields__ and item['string'] is not None:
                return str(item['string'])
            if 'int' in item.__fields__ and item['int'] is not None:
                return str(item['int'])
            if 'double' in item.__fields__ and item['double'] is not None:
                return str(item['double'])
            if 'boolean' in item.__fields__ and item['boolean'] is not None:
                return str(item['boolean'])
            return None
        # If it's not a struct, it's a simple scalar value
        return str(item)

    # Register the function as a UDF that returns a StringType
    extract_value_udf = udf(extract_value_from_item, StringType())

    columns_with_types = [
        ('icao24', StringType()), ('callsign', StringType()), ('origin_country', StringType()),
        ('time_position', IntegerType()), ('last_contact', IntegerType()), ('longitude', FloatType()),
        ('latitude', FloatType()), ('baro_altitude', FloatType()), ('on_ground', BooleanType()),
        ('velocity', FloatType()), ('true_track', FloatType()), ('vertical_rate', FloatType()),
        ('sensors', StringType()), ('geo_altitude', FloatType()), ('squawk', StringType()),
        ('spi', BooleanType()), ('position_source', IntegerType())
    ]

    # Apply the UDF to each item in the array, then cast to the final correct type.
    # This makes the main transformation logic much cleaner.
    select_exprs = [
        extract_value_udf(col('state_data').getItem(i)).cast(dtype).alias(name)
        for i, (name, dtype) in enumerate(columns_with_types)
    ]
    
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
              .partitionBy("year", "month") \
              .parquet(s3_output_path)
              
    print(f"Data Written Successfully to {s3_output_path}")

except Exception as e:
    print(f"An error occurred: {e}")
    # Do NOT commit the job on failure. Let it fail so it can be retried.
    raise e

# --- 5. Commit the job ONLY on successful completion ---
# This saves the bookmark state for the next run.
job.commit()
