from typing import NoReturn
from zipfile import ZipFile

import io
import os
import boto3
import requests

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
}

def save_taxi_zone_lookup() -> NoReturn:

    url: str = os.environ['TAXI_ZONE_LOOKUP_URL']
    prefix = os.environ['BUCKET_PREFIX']
    bucket = os.environ['BUCKET_NAME']

    s3_client = boto3.client('s3')

    url_response = requests.get(url, headers=HEADERS, stream=True)
    url_response.raise_for_status()

    csv_name = url.split('/')[-1]
    s3_key = f'{prefix}/{csv_name}'

    s3_client.upload_fileobj(url_response.content, bucket, s3_key)

def save_taxi_zone_shapefile() -> NoReturn:

    url: str = os.environ['TAXI_ZONE_SHAPEFILE_URL']
    prefix = os.environ['SHAPEFILE_PREFIX']
    bucket = os.environ['BUCKET_NAME']

    s3_client = boto3.client('s3')

    url_response = requests.get(url, headers=HEADERS, stream=True)
    url_response.raise_for_status()

    with ZipFile(io.BytesIO(url_response.content)) as zip_file:
        for file_info in zip_file.infolist():
            if not file_info.is_dir():
                with zip_file.open(file_info) as file:
                    s3_client.upload_fileobj(file, bucket, f'{prefix}/{file.name}')

def handler(event, context):

    try:

        print('Start Lambda Ingestion')

        print('Downloading CSV file and saving them on raw layer')
        save_taxi_zone_lookup()

        print('Downloading Shapefiles and saving them on raw layer')
        save_taxi_zone_shapefile()

        print('End Lambda Ingestion')

        return 'Geo data ingested successfully'

    except Exception as error:
        print("Error: ", error)
        raise error
