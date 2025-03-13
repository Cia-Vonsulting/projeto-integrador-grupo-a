# eEDB-015-2025-1
Repository dedicated to the discipline eEDB-015/2025-1 from the PECE-POLI/USP post-graduate course in data engineer and big data.

## download the TLC

The dataset used in this project is available at the [TLC site](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

## Loading web scrapping layer to be used in the Lambda ingestion

> aws lambda publish-layer-version --layer-name web-scrapping-python312 --description "bs4 access by python 3.12" --zip-file fileb://web-scrapping-layer.zip --compatible-runtimes python3.12