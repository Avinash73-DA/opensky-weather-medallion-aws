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
    
    ## Layer 1: Bronze Ingestion (Run in parallel)
    
    lambda_flights = LambdaInvokeFunctionOperator(
        task_id="invoke_lambda_flights",
        function_name="flights_data_pull",
    )
    
    lambda_weather = LambdaInvokeFunctionOperator(
        task_id="invoke_lambda_weather",
        function_name="weatherDataPull",
    )

    ## Layer 2: Silver Transformation
    
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
    
    ## Layer 3: Gold Layer Aggregation (Run in parallel)
    
    gold_weather_impact = GlueJobOperator(
        task_id='gold_weather_impact',
        job_name='WeatherImpactOnFlights',
        wait_for_completion=True
    )
    
    flight_weather = GlueJobOperator(
        task_id='flight_weather',
        job_name='flight_weather_snapshot',
        wait_for_completion=True
    )
    
    weather_history = GlueJobOperator(
        task_id='weather_history',
        job_name='CityWeatherHistory',
        wait_for_completion=True
    )
    
    # Step 1: Specific Lambda ingestion tasks trigger the first set of Silver jobs.
    lambda_flights >> process_plane_data
    lambda_weather >> process_weather_data
    
    # Step 2: The enrichment job (final Silver task) runs only after the initial Silver jobs are done.
    [process_plane_data, process_weather_data] >> process_enriched_data
    
    # Step 3: All Gold jobs run in parallel, but only after the final Silver job is complete.
    process_enriched_data >> [gold_weather_impact, flight_weather ,weather_history]