from airflow.models.dag import DAG
from datetime import datetime,timedelta
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator

default_args = {
    'owner':'Avinash',
    'depends_on_past':False,
    'email_on_failure':False,
    'retries':1,
    'retry_delay':timedelta(minutes=5),
}

with DAG(
    dag_id = "lambda_bronze_ingestion_dag",
    default_args=default_args,
    description="DAG to invoke Lambda function for bronze ingestion",
    schedule_interval="@daily",
    start_date=datetime(2025,8,17),
    catchup=False,
    tags=["aws","lambda","bronze","ingestion"]
) as dag:
    
    lambda_flights = LambdaInvokeFunctionOperator(
        task_id="invoke_lambda_flights",
        function_name="flights_data_pull",
    )
    
    lambda_weather = LambdaInvokeFunctionOperator(
        task_id="invoke_lambda_weather",
        function_name="weatherDataPull",
    )
    
    lambda_flights >> lambda_weather
