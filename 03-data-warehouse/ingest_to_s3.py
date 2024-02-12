from pathlib import Path 
import pandas as pd 
import numpy as np
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
from prefect_aws.s3 import s3_upload

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
        url = f"{base_url}/{color}_tripdata_{year}-{month:02}.{extension}"
        print(url)
        df = pd.read_parquet(url)
        
        df_list.append(df)

        print(f'Imported file: {color}_tripdata_{year}_{month:02}')

    full_df = pd.concat(df_list)

    

    print(full_df.dtypes, full_df.columns)

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
    
    print(len(df.query('fare_amount == 0')), len(raw_df.query('fare_amount == 0')))
    return df

@flow(log_prints=True, retries=3)
async def s3_ingest_data(df, color: str) -> None:

    aws_credentials_block = await AwsCredentials.load("aws-credentials")

    credentials = AwsCredentials(
        aws_access_key_id=aws_credentials_block.aws_access_key_id,
        aws_secret_access_key=aws_credentials_block.aws_secret_access_key,
    )

    sorted_df = df.sort_values(by=['lpep_pickup_date'])
    for pickup_dt in sorted_df['lpep_pickup_date'].unique():
        temp_df = df.query('lpep_pickup_date == @pickup_dt').copy()

        
        
        year = pickup_dt.year
        month = pickup_dt.month
        day = pickup_dt.day

        os.makedirs(f"data/{color}/{year}/{month:02}/{day}", exist_ok=True)
        

        path = fr"data/{color}/{year}/{month:02}/{day}/{color}_tripdata_{year}-{month:02}-{day:02}.parquet"    

        temp_df.to_parquet(path, index=False)

        with open(path, "rb") as file:
            key = await s3_upload(data = file.read(),
                        bucket=f'mage-demo-bucket-mkhan',
                        aws_credentials=credentials,
                    key=f"{path}", )

@flow(log_prints=True)
def green_flow():
    months = np.arange(1,13)
    year = 2022
    color = 'green'
    base_url = r"https://d37ci6vzurychx.cloudfront.net/trip-data"
    extension = 'parquet'

    raw_df = fetch_monthly_data(base_url, color, extension, months, year)

    df = transform(raw_df)

    s3_ingest_data(df, color)


if __name__ == "__main__":
    green_flow()
