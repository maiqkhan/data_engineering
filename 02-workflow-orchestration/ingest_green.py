from pathlib import Path 
import pandas as pd 
from datetime import timedelta
from prefect import flow, task 
from prefect.tasks import task_input_hash
from prefect_aws import AwsCredentials
from prefect_aws.s3 import s3_download
from prefect_aws.s3 import S3Bucket
from prefect_sqlalchemy import SqlAlchemyConnector
from sqlalchemy.engine.url import URL
from sqlalchemy.engine import create_engine
from dotenv import load_dotenv


from datetime import datetime as dt
import os
import io 
import re
from typing import List


@task(log_prints=True, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def fetch_monthly_data(base_url: str, color: str, extension:str, months: List[int], year: int) -> pd.DataFrame:
    """Read taxi data from web into pandas Dataframe"""
    df_list = []
    for month in months:
        url = f"{base_url}/{color}/{color}_tripdata_{year}-{month:02}.{extension}"
        print(url)
        if url[-2:] == 'gz':
            df = pd.read_csv(url, compression='gzip')
        else:
            df = pd.read_csv(url)

        df_list.append(df)

        print(f'Imported file: {color}_tripdata_{year}_{month:02}')

    full_df = pd.concat(df_list)



    print(full_df.shape)

    return full_df


@task(retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def transform(raw_df: pd.DataFrame) -> pd.DataFrame:

    raw_df['passenger_count'] = raw_df['passenger_count'].fillna(0) # Assume missing passenger_count = 0
    raw_df['trip_distance'] = raw_df['trip_distance'].fillna(0) # Assume missing trip_distance = 0
    
    df = raw_df.query('passenger_count > 0 and trip_distance > 0').copy()  # Keep only rows that have a trip_distance and passenger_count

    df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime']) # Convert pickup timestamp from string to datetime data type
    df['lpep_pickup_date'] = df['lpep_pickup_datetime'].dt.date   #.apply(lambda x: x.date())  Create date column based on pickup timestamp

    df = df.rename(columns = lambda x: re.sub('([a-z])([A-Z])', r'\1_\2', x).lower())  #convert column name to camel case

    print(df.shape[0]) # Number of rows, after trip distance and passenger_count <= 0 is filtered out
    print(df['vendor_id'].unique())    #Existing values of Vendor ID
    print(len(df['lpep_pickup_date'].unique()))
    return df

@flow(log_prints=True, retries=3)
async def ingest_data(schema_name:str, table_name: str, df) -> None:

    connection_block = await SqlAlchemyConnector.load("postgres-connector")

    with connection_block.get_connection(begin=False) as engine:
        df.head(n=0).to_sql(name=table_name, schema=schema_name, con=engine, if_exists='replace')

        df.to_sql(name=table_name, schema=schema_name, con=engine, if_exists='append', chunksize=10_000)
        print(f'Data pushed to {schema_name}.{table_name} table in Postgres')


@flow(log_prints=True)
def green_flow():
    months = [10, 11, 12]
    year = 2020
    color = 'green'
    base_url = r"https://github.com/DataTalksClub/nyc-tlc-data/releases/download"
    extension = 'csv.gz'

    schema_name = "prefect"
    table_name = "green_taxi"

    raw_df = fetch_monthly_data(base_url, color, extension, months, year)

    df = transform(raw_df)

    ingest_data(schema_name, table_name, df)




if __name__ == "__main__":
    green_flow()
