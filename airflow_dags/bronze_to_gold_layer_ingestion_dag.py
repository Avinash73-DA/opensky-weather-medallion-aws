from airflow.models.dag import DAG
from datetime import datetime,timedelta
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator

default_args = {
    'owner':'Avinash',
    'depends_on_past':False,
    'email_on_failure':False,
    'retries':1,
    'retry_delay':timedelta(minutes=5),
}

with DAG(
    dag_id = "bronze_to_silver_pipeline_v2", # Renamed for clarity
    default_args=default_args,
    description="DAG to ingest data and process it to a Silver layer",
    schedule_interval=timedelta(minutes=60),
    start_date=datetime(2025,8,17),
    catchup=False,
    tags=["aws","lambda","glue","silver","ingestion"]
) as dag:
    
    ## Bronze Layer Ingestion (run in parallel)
    lambda_flights = LambdaInvokeFunctionOperator(
        task_id="invoke_lambda_flights",
        function_name="flights_data_pull",
    )
    
    lambda_weather = LambdaInvokeFunctionOperator(
        task_id="invoke_lambda_weather",
        function_name="weatherDataPull",
    )

    ## Silver Layer Transformation
    process_plane_data = GlueJobOperator(
        task_id="process_plane_data",
        job_name="planes_silver",
        wait_for_completion=True
    )
    
    process_weather_data = GlueJobOperator(
        task_id="process_weather_data",
        job_name="openweather_silver",
        wait_for_completion=True
    )
    
    ## Final Enrichment Job
    process_enriched_data = GlueJobOperator(
        task_id="process_enriched_data",
        job_name="open_sky_weather_enriched_silver",
        wait_for_completion=True
    )
    
    # CORRECTED DEPENDENCIES: Define the logical data flow
    
    # Each processing job depends on its specific ingestion job
    lambda_flights >> process_plane_data
    lambda_weather >> process_weather_data
    
    # The final enrichment job depends on the completion of the two processing jobs
    [process_plane_data, process_weather_data] >> process_enriched_data