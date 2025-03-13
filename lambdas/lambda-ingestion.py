from bs4 import BeautifulSoup
from io import BytesIO

import os
import requests
import boto3

def handler(event, context):

    url = os.environ['NYC_TLC_URL']
    bucket = os.environ['BUCKET_NAME']
    prefix = os.environ['BUCKET_PREFIX']

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
    }

    try:

        response = requests.get(url, headers=headers)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, features='html.parser')

        parquet_links: list = []

        for link in soup.find_all('a', href=True):
            href = link['href']
            if href.endswith('.parquet'):
                parquet_links.append(href)

        yellow_tripdata_links = \
            list(filter(lambda href: 'yellow_tripdata' in href, parquet_links))
        
        s3_client = boto3.client('s3')

        for link in yellow_tripdata_links:
            link_response = requests.get(link, headers=headers, stream=True)
            link_response.raise_for_status()

            parquet_name = link.split('/')[-1]
            s3_key = prefix + parquet_name

            with BytesIO(link_response.content) as file_buffer:

                for chunk in link_response.iter_content(chunk_size=8192):
                    file_buffer.write(chunk)

                file_buffer.seek(0)  # Reset buffer position to start

                s3_client.upload_fileobj(file_buffer, bucket, s3_key)

    except Exception as error:
        print("Error: ", error)
        raise error
