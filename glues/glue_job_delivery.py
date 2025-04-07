
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

def write_table(df, target_bucket, target_database, target_table):
    print('Writing the transformed dataframe into the delivery layer')

    path = f's3://{target_bucket}/{target_database}/{target_table}/'
    df.write. \
        mode('append'). \
        format('parquet'). \
        option('compression', 'snappy'). \
        option('path', path). \
        partitionBy('partition_dt'). \
        saveAsTable(f'{target_database}.{target_table}')

def main():

    print('Initing Trusted Glue Job')
    
    args_list = [
        'sourcedatabase',
        'targetbucket',
        'targetdatabase',
        'targettable',
        'month',
        'tables'
    ]

    args = solve_args(args_list)

    print('Retrieving job parameter: \n', args)

    source_database  = args['sourcedatabase']
    target_bucket    = args['targetbucket']
    target_database  = args['targetdatabase']
    target_table     = args['targettable']
    tables           = args['tables']
    month            = args['month']

    tables_list = tables.split(",")
    month_where = f"AND partition_dt = '{month}'" if month is not None else ""

    if 'summarized_trip_cost' in tables_list:

        print('Reading data from table yellow_tripdata on trusted layer')

        query = f'''
            SELECT
                round(sum(fare_amount),2) sum_fare_amount,
                round(sum(extra),2) sum_extra,
                round(sum(mta_tax),2) sum_mta_tax,
                round(sum(tip_amount),2) sum_tip_amount,
                round(sum(tolls_amount),2) sum_tolls_amount,
                round(sum(improvement_surcharge),2) sum_improvement_surcharge,
                round(sum(congestion_surcharge),2) sum_congestion_surcharge,
                round(sum(total_amount),2) sum_total_amount,
                round(avg(total_amount),2) avg_total_amount,
                partition_dt
            FROM 
                {source_database}.yellow_tripdata
            WHERE 
                total_amount > 0
                {month_where}
            GROUP BY
                partition_dt
        ''' 

        print(f"Query summarized_trip_cost: {query}")
        df = spark.sql(query).cache()

        print('Loading table summarized_trip_cost')

        write_table(df, target_bucket, target_database, 'summarized_trip_cost')

    if 'trip_peak_time' in tables_list:

        print('Reading data from table yellow_tripdata on trusted layer')

        query = f'''
            SELECT 
                date_format(pickup_datetime, 'HH:00:00') AS trip_hour,
                count(*) trips_per_hour,
                partition_dt
            FROM 
                {source_database}.yellow_tripdata
            WHERE
                dropoff_datetime > pickup_datetime
                {month_where}
            GROUP BY
                date_format(pickup_datetime, 'HH:00:00'),
                partition_dt
        '''

        print(f"Query trip_peak_time: {query}")
        df = spark.sql(query).cache()

        print('Loading table trip_peak_time')

        write_table(df, target_bucket, target_database, 'trip_peak_time')

    if 'trip_peak_zones' in tables_list:

        print('Reading data from table yellow_tripdata on trusted layer')

        query = f'''
            SELECT 
                pickup_neighborhood,
                pickup_zone,
                pickup_service_zone,
                count(*) trips_per_zone,
                partition_dt
            FROM 
                {source_database}.yellow_tripdata
            WHERE
                dropoff_datetime > pickup_datetime
                {month_where}
            GROUP BY
                partition_dt, 
                pickup_neighborhood,
                pickup_zone,
                pickup_service_zone
        '''

        print(f"Query trip_peak_zones: {query}")
        df = spark.sql(query).cache()

        print('Loading table trip_peak_zones')

        write_table(df, target_bucket, target_database, 'trip_peak_zones')

    print('Ending Trusted Glue Job')

main()

job.commit()