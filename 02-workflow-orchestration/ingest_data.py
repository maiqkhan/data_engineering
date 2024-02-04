import pandas as pd 
import os

from sqlalchemy import create_engine
from datetime import timedelta
from prefect import flow, task 
from prefect.tasks import task_input_hash
from prefect_sqlalchemy import SqlAlchemyConnector

@task(log_prints=True, retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def extract_data(url):
    csv_name = 'output.csv'

    os.system(f"wget {url} -O {csv_name}")

    df = pd.read_csv(csv_name, compression='gzip')

    print(df.columns)
    try:
        df = df.rename(columns={'lpep_pickup_datetime': 'tpep_pickup_datetime',
                        'lpep_dropoff_datetime': 'tpep_dropoff_datetime'})
    except:
        pass
    
    df = df[['VendorID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime',
       'passenger_count', 'trip_distance', 'RatecodeID', 'store_and_fwd_flag',  'PULocationID', 
       'DOLocationID',  'payment_type',   'fare_amount', 'extra', 'mta_tax',
       'tip_amount', 'tolls_amount', 'improvement_surcharge', 'total_amount', 'congestion_surcharge' ]].copy()

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    return df


@task(log_prints=True, retries=3)
def transform_data(df):
    print(f"pre:missing passenger count {df['passenger_count'].isin([0]).sum()}")

    df = df[df['passenger_count'] != 0].copy() 

    print(f"post: missing passenger count: {df['passenger_count'].isin([0]).sum()}")

    return df 


@task(log_prints=True, retries=3)
def ingest_data( table_name, df):

    connection_block = SqlAlchemyConnector.load("postgres-connector")

    with connection_block.get_connection(begin=False) as engine:
        df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

        df.to_sql(name=table_name, con=engine, if_exists='append', chunksize=10_000)
        print(f'Data pushed to {table_name} table in Postgres')



@flow(name='Ingest Flow')
def main_flow(table_name: str):
    
    csv_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz'
    raw_data = extract_data(csv_url)
    data = transform_data(raw_data)

    ingest_data(table_name, data)

    
if __name__ == "__main__":
    main_flow("yellow_taxi_trips")