import json
import asyncio
import aiohttp
import time
import os
from datetime import datetime
import nest_asyncio
import boto3

nest_asyncio.apply()

s3_client = boto3.client('s3')

async def fetch_weather_by_city(session, city, api_key):
    url = f'https://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}'
    try:
        async with session.get(url, timeout=10) as response:
            response.raise_for_status()
            return await response.json()
    except Exception as e:
        return {'error': str(e), 'city': city}

async def main_fetcher(locations, api_key):
    semaphore = asyncio.Semaphore(50)
    async with aiohttp.ClientSession() as session:
        tasks = []
        for city in locations:
            async def fetch_with_semaphore(city):
                async with semaphore:
                    return await fetch_weather_by_city(session, city, api_key)
            tasks.append(fetch_with_semaphore(city))
        
        results = await asyncio.gather(*tasks)
        return results

def lambda_handler(event, context):
    start_time = time.time()
    
    api_key = os.environ.get('OPEN_WEATHER_KEY')
    target_bucket = os.environ.get('TARGET_BUCKET')
    source_bucket = os.environ.get('SOURCE_BUCKET')
    source_key = os.environ.get('SOURCE_KEY')
    
    if not all([api_key, target_bucket, source_bucket, source_key]):
        return {'statusCode': 400, 'body': json.dumps('Error: Missing environment variables.')}

    try:
        s3_object = s3_client.get_object(Bucket=source_bucket, Key=source_key)
        content = s3_object['Body'].read().decode('utf-8')
        target_locations = [line.strip() for line in content.splitlines() if line.strip()]
        print(f"Loaded {len(target_locations)} locations from s3://{source_bucket}/{source_key}")
    except Exception as e:
        print(f"Error reading from S3: {e}")
        return {'statusCode': 500, 'body': json.dumps(f"Error reading from S3: {e}")}

    if not target_locations:
        return {'statusCode': 200, 'body': json.dumps('No locations to process.')}

    all_results = asyncio.run(main_fetcher(target_locations, api_key))
    
    successful_data = [res for res in all_results if 'error' not in res]
    failed_requests = [res for res in all_results if 'error' in res]

    output_key = ""
    if successful_data:
        now = datetime.utcnow()
        output_key = f"bronze/openweather_api/wether_data{now.strftime("%Y%m%d_%H%M%S")}.json"
        
        try:
            s3_client.put_object(
                Bucket=target_bucket,
                Key=output_key,
                Body=json.dumps(successful_data, indent=4)
            )
            print(f"Successfully saved {len(successful_data)} results to s3://{target_bucket}/{output_key}")
        except Exception as e:
            print(f"Error writing to S3: {e}")
            return {'statusCode': 500, 'body': json.dumps(f"Error writing to S3: {e}")}
            
    duration = time.time() - start_time
    summary = {
        "duration_seconds": f"{duration:.2f}",
        "successful_calls": len(successful_data),
        "failed_calls": len(failed_requests),
        "output_location": f"s3://{target_bucket}/{output_key}" if successful_data else "N/A"
    }

    return {
        'statusCode': 200,
        'body': json.dumps(summary)
    }
