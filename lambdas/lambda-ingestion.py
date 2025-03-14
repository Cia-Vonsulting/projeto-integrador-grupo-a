from io import BytesIO
from typing import NoReturn

import os
import requests
import boto3

def scrap_and_save_parquets(urls: list[str]) -> NoReturn:

    bucket = os.environ['BUCKET_NAME']
    prefix = os.environ['BUCKET_PREFIX']

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
    }

    s3_client = boto3.client('s3')

    for url in urls:
        url_response = requests.get(url, headers=headers, stream=True)
        url_response.raise_for_status()

        parquet_name = url.split('/')[-1]
        s3_key = prefix + parquet_name

        with BytesIO(url_response.content) as file_buffer:

            for chunk in url_response.iter_content(chunk_size=8192):
                file_buffer.write(chunk)

            file_buffer.seek(0)  # Reset buffer position to start

            s3_client.upload_fileobj(file_buffer, bucket, s3_key)

    # TODO: delete messages from queue

def handler(event, context):

    try:

        print('Start Lambda Ingestion')

        print('Extracting the received list of URLs')
        urls = set(map(lambda record: record['body'], event['Records']))
        print('URLs: ', urls)

        print('Scraping the parquet files and saving them on the raw layer')
        scrap_and_save_parquets(urls)

        print('End Lambda Ingestion')

        return {'message': 'success'}

    except Exception as error:
        print("Error: ", error)
        raise error
