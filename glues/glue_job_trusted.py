
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

import sys

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

def solve_args(args_list):
    return getResolvedOptions(sys.argv, args_list)

def main():

    print('Initing Trusted Glue Job')
    
    args_list = [
        'sourcebucket',
        'sourceprefix',
        'sourcedatabase',
        'sourcezonetable',
        'targetbucket',
        'targetdatabase',
        'targettable',
        'month',
    ]

    args = solve_args(args_list)

    print('Retrieving job parameter: \n', args)

    source_bucket     = args['sourcebucket']
    source_prefix     = args['sourceprefix']
    source_database   = args['sourcedatabase']
    source_zone_table = args['sourcezonetable']
    target_bucket     = args['targetbucket']
    target_database   = args['targetdatabase']
    target_table      = args['targettable']
    month             = args['month']

    print('Reading yellow trip data parquet and creating a temp view')

    parquet_path = f's3://{source_bucket}/{source_prefix}/{month}.parquet'
    spark.read.parquet(parquet_path).createOrReplaceTempView('temp_yellow_tripdata')

    print('Applying trasnformations on the raw dataset')

    df = spark.sql(
    f'''
        SELECT
            CAST(yt.VendorID AS INT) AS vendorid,
            CAST(yt.tpep_pickup_datetime AS TIMESTAMP) AS pickup_datetime,
            CAST(yt.tpep_dropoff_datetime AS TIMESTAMP) AS dropoff_datetime,
            CAST(yt.passenger_count AS INT) AS passenger_count,
            CAST(yt.trip_distance AS DOUBLE) AS trip_distance,
            CAST(yt.RatecodeID AS INT) AS rate_code_id,
            CAST(yt.store_and_fwd_flag AS STRING) AS store_and_fwd_flag,
            CAST(puz.borough AS STRING) AS pickup_neighborhood,
            CAST(puz.zone AS STRING) AS pickup_zone,
            CAST(puz.service_zone AS STRING) AS pickup_service_zone,
            CAST(doz.borough AS STRING) AS dropoff_neighborhood,
            CAST(doz.zone AS STRING) AS dropoff_zone,
            CAST(doz.service_zone AS STRING) AS dropoff_service_zone,
            CAST(yt.payment_type AS INT) AS payment_type,
            CAST(yt.fare_amount AS DOUBLE) AS fare_amount,
            CAST(yt.extra AS DOUBLE) AS extra,
            CAST(yt.mta_tax AS DOUBLE) AS mta_tax,
            CAST(yt.tip_amount AS DOUBLE) AS tip_amount,
            CAST(yt.tolls_amount AS DOUBLE) AS tolls_amount,
            CAST(yt.improvement_surcharge AS DOUBLE) AS improvement_surcharge,
            CAST(yt.total_amount AS DOUBLE) AS total_amount,
            CAST(yt.congestion_surcharge AS DOUBLE) AS congestion_surcharge,
            CAST(yt.airport_fee AS DOUBLE) AS airport_fee,
            '{month}' AS partition_dt
        FROM
            temp_yellow_tripdata yt
            INNER JOIN {source_database}.{source_zone_table} puz ON CAST(yt.PULocationID AS INT) = puz.locationid
            INNER JOIN {source_database}.{source_zone_table} doz ON CAST(yt.DOLocationID AS INT) = doz.locationid
    '''
    ).cache()

    print('Writing the transformed dataframe into the trusted layer')

    trusted_path = f's3://{target_bucket}/{target_database}/{target_table}/'
    df.write. \
        mode('append'). \
        format('parquet'). \
        option('compression', 'snappy'). \
        option('path', trusted_path). \
        partitionBy('partition_dt'). \
        saveAsTable(f'{target_database}.{target_table}')

    print('Ending Trusted Glue Job')

main()

job.commit()