from io import BytesIO
from typing import NoReturn
from bs4 import BeautifulSoup

import re
import os
import boto3
import requests

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
}

def get_parquet_urls(trip_type: str, year: str, month: str) -> list[str]:

    url: str = os.environ['NYC_TLC_URL']
    href_pattern = re.compile(f'^.+{trip_type}_{year}-{month}\.parquet\s?$')

    response = requests.get(url, headers=HEADERS)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, features='html.parser')

    links = soup.find_all('a', href=href_pattern)
    urls = list(map(lambda link: link['href'].strip(), links))

    return urls

def scrap_and_save_parquets(urls: list[str], trip_type: str) -> NoReturn:

    bucket = os.environ['BUCKET_NAME']
    prefix = os.environ['BUCKET_PREFIX']

    s3_client = boto3.client('s3')

    for url in urls:

        url_response = requests.get(url, headers=HEADERS, stream=True)
        url_response.raise_for_status()

        parquet_name = url.split('/')[-1]
        parquet_period = parquet_name.split('_')[-1]
        s3_key = f'{prefix}/{trip_type}/{parquet_period}'

        with BytesIO(url_response.content) as file_buffer:

            for chunk in url_response.iter_content(chunk_size=8192):
                file_buffer.write(chunk)

            file_buffer.seek(0)

            s3_client.upload_fileobj(file_buffer, bucket, s3_key)

def handler(event, context):

    try:

        print('Start Lambda Ingestion')

        print('Reading parameters: ', event)
        trip_type = event['trip_type']
        year = event['year']
        month = event['month'] if 'month' in event else '\d{2}'

        print('Extracting the desired bunch of parquet URLs')
        parquet_urls = get_parquet_urls(trip_type, year, month)
        print('parquet URLs: ', parquet_urls)

        print('Downloading parquet files and saving them on raw layer')
        scrap_and_save_parquets(parquet_urls, trip_type)

        print('End Lambda Ingestion')

        return 'success'

    except Exception as error:
        print("Error: ", error)
        raise error
