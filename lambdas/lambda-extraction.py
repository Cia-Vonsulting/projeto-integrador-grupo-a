from bs4 import BeautifulSoup
from typing import NoReturn
from itertools import batched

import os
import boto3
import requests

def get_stored_parquets() -> set[str]:
    
    bucket = os.environ['BUCKET_NAME']
    prefix = os.environ['BUCKET_PREFIX']

    s3_client = boto3.client('s3')

    object_list = s3_client.list_objects_v2(bucket, prefix=prefix)

    return set(map(lambda content: content['Key'], object_list['contents']))

def get_web_site_data() -> dict[str: str]:

    url = os.environ['NYC_TLC_URL']
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
    }

    response = requests.get(url, headers=headers)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, features='html.parser')

    links: list = []

    for link in soup.find_all('a', href=True):
        href = link['href']
        if href.endswith('.parquet'):
            links.append(href)

    filtered_links = set(filter(lambda href: 'yellow_tripdata' in href, links))

    web_site_data: dict = {}

    for link in filtered_links:
        parquet_name = link.split('/')[-1]
        web_site_data[parquet_name] = link

    return web_site_data

def send_urls_to_queue(urls: list[str]) -> NoReturn:
    
    queue_url = os.environ['QUEUE_URL']

    sqs_client = boto3.client('sqs')

    url_batches = list(batched(urls, 10))

    for url_batch in url_batches:

        entries = [{'Id': str(id), 'MessageBody': url} for id, url in enumerate(url_batch)]
        print('entries', entries)

        response = sqs_client.send_message_batch(queue_url, entries)

        if 'Failed' in response:
            print(f'Failed messages: {len(response['Failed'])}')
            for failure in response['Failed']:
                print(f'ID: {failure['Id']}, Error: {failure['Message']}')
        else:
            print(f'All {len(url_batch)} URLs sent successfully!')

    print(f'All {len(url_batches)} batches sent successfully')

def handler(event, context) -> NoReturn:

    try:

        print('Start Lambda Extraction')

        print('Loading metadata about downloaded and new parquets')
        web_site_data = get_web_site_data()
        web_site_parquets = set(web_site_data.keys())
        stored_parquets = get_stored_parquets()

        print('Finding new parquets urls to scrap data from the NYC TLC web site')
        new_parquets = web_site_parquets - stored_parquets
        parquet_urls = [web_site_data[parquet] for parquet in new_parquets]
        print('New parquet urls: ', parquet_urls)

        print('Sending new parquet URLs to the SQS queue')
        send_urls_to_queue(parquet_urls)

        print('End Lambda Extraction')

        return {'message': 'success'}

    except Exception as error:
        print("Error: ", error)
        raise error
