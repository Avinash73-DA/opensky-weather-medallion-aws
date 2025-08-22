import json
import os
import boto3
import requests
from datetime import datetime

# Initialize the S3 client outside the handler function.
# This allows AWS Lambda to reuse the connection for better performance.
s3_client = boto3.client('s3')

def lambda_handler(event, context):
    """
    AWS Lambda handler function to:
    1. Fetch flight data from the OpenSky Network API.
    2. Save the raw JSON data to an S3 bucket (Bronze Layer).
    """
    # --- 1. Get Environment Variables ---
    # These will be configured in the Lambda function's settings in the AWS Console.
    CLIENT_ID = os.environ.get("OPENSKY_CLIENT_ID")
    CLIENT_SECRET = os.environ.get("OPENSKY_CLIENT_SECRET")
    S3_BUCKET = os.environ.get("S3_BUCKET_NAME")

    if not all([CLIENT_ID, CLIENT_SECRET, S3_BUCKET]):
        print("ERROR: Missing environment variables.")
        # Return a failure response
        return {
            'statusCode': 500,
            'body': json.dumps('Configuration Error: Missing environment variables.')
        }
    
    # --- 2. Request Access Token ---
    token_url = 'https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token'
    token_data = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET
    }
    token_header = {
        "Content-Type": "application/x-www-form-urlencoded"
    }

    try:
        tk_response = requests.post(token_url, data=token_data, headers=token_header)
        tk_response.raise_for_status() # Raise an exception for bad status codes
        access_token = tk_response.json().get('access_token')
        print("Access token retrieved successfully.")
    except requests.exceptions.RequestException as e:
        print(f"ERROR: Error retrieving access token: {e}")
        return {
            'statusCode': 502, # Bad Gateway
            'body': json.dumps(f'Failed to retrieve access token: {e}')
        }

    # --- 3. Pull Flight Data ---
    states_url = 'https://opensky-network.org/api/states/all'
    headers = {
        "Authorization": f"Bearer {access_token}"
    }

    try:
        response = requests.get(states_url, headers=headers)
        response.raise_for_status()
        flight_data = response.json()
        print("Flight data retrieved successfully.")
    except requests.exceptions.RequestException as e:
        print(f"ERROR: Could not fetch flight data: {e}")
        return {
            'statusCode': 502,
            'body': json.dumps(f'Failed to fetch flight data: {e}')
        }

    # --- 4. Upload Data to S3 ---
    time_field = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    file_name = f"airplane_data_{time_field}.json"
    s3_key = f"bronze/opensky_api/{file_name}"
    
    try:
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=json.dumps(flight_data)
        )
        print(f"Successfully uploaded data to s3://{S3_BUCKET}/{s3_key}")
    except Exception as e:
        print(f"ERROR: Error uploading data to S3: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error uploading to S3: {e}')
        }

    # --- 5. Return a Success Response ---
    return {
        'statusCode': 200,
        'body': json.dumps(f'Successfully uploaded {s3_key} to {S3_BUCKET}')
    }