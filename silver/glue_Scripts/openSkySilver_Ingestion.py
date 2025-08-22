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
    StructType, StructField, StringType, ArrayType, LongType,
    IntegerType, FloatType, BooleanType
)

# --- 1. Initialization (No Change) ---
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
    # --- 2. Optimized Read with Explicit Schema ---
    state_vector_schema = ArrayType(ArrayType(StringType()))
    input_schema = StructType([
        StructField("time", LongType(), True),
        StructField("states", state_vector_schema, True)
    ])

    df_raw = spark.read.format('json') \
                .schema(input_schema) \
                .option('multiline', True) \
                .load(SOURCE_PATH)

    exploed_view = df_raw.select(explode('states').alias('state_data'))
    print("Data Loaded Successfully")

    # --- 3. Streamlined Transformation Logic ---
    columns_with_types = [
        ('icao24', StringType()), ('callsign', StringType()), ('origin_country', StringType()),
        ('time_position', IntegerType()), ('last_contact', IntegerType()), ('longitude', FloatType()),
        ('latitude', FloatType()), ('baro_altitude', FloatType()), ('on_ground', BooleanType()),
        ('velocity', FloatType()), ('true_track', FloatType()), ('vertical_rate', FloatType()),
        ('sensors', StringType()), ('geo_altitude', FloatType()), ('squawk', StringType()),
        ('spi', BooleanType()), ('position_source', IntegerType())
    ]

    select_exprs = [
        col('state_data').getItem(i).cast(dtype).alias(name)
        for i, (name, dtype) in enumerate(columns_with_types)
    ]
    
    df_transformed = exploed_view.select(*select_exprs)

    # --- 4. Add Partition Columns ---
    df_with_ts = df_transformed.withColumn("processed_timestamp", from_utc_timestamp(current_timestamp(), "Asia/Kolkata"))

    df_final = df_with_ts.withColumn("year", year(col("processed_timestamp"))) \
                         .withColumn("month", month(col("processed_timestamp"))) \
                         .withColumn("day", dayofmonth(col("processed_timestamp")))
    
    print("Data Transformed Successfully")

    s3_output_path = f"s3://{BUCKET}/silver/planes_data/" 
    
    df_final.write.mode("overwrite") \
            .partitionBy("year", "month") \
            .parquet(s3_output_path)
            
    print(f"Data Written Successfully to {s3_output_path}")

except Exception as e:
    print(f"An error occurred: {e}")
    job.commit()
    raise e

job.commit()