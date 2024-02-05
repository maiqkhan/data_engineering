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


@flow(log_prints=True, retries=3)
async def extract_from_s3(color: str, year: int, month: int) -> Path:
    """Downlaod trip data from S3"""

   
    s3_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"

    aws_credentials_block = await AwsCredentials.load("aws-credentials")

    credentials = AwsCredentials(
        aws_access_key_id=aws_credentials_block.aws_access_key_id,
        aws_secret_access_key=aws_credentials_block.aws_secret_access_key,
    )

   
    data = await s3_download(
                bucket=f'mage-demo-bucket-mkhan',
                aws_credentials=credentials,
                key=f"{s3_path}", )
    
    return data
    

@task(log_prints=True, retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def transform(data : bytes) -> pd.DataFrame:
    """Data Cleanse"""
    # s = str(data, 'latin-1')

    # data_str = io.BytesIO(s)
    df = pd.read_parquet(io.BytesIO(data))

    print('###############################The data is read')
    df['passenger_count'] = df['passenger_count'].fillna(0)

    return df


@task(log_prints=True)
def write_rds(df, host, port, database, user, password) -> None:
    """Write to Amazon Redshift"""

   

   

    conn = URL.create(
        drivername = 'redshift+redshift_connector',
        host = host,
        port = port,
        database = database,
        username = user,
        password = password
    )
    
    engine = create_engine(conn)
    
    print(df.shape[0])

    
    df.head(n=0).to_sql(name='rides', con=engine, if_exists='replace', index=False)

    start_time = dt.now()

    df_head = df.iloc[:100_000,:]
    df_head.to_sql("rides", con=engine, if_exists='append', chunksize=50, method='multi', index=False)

    end_time = dt.now()
    
    print(end_time - start_time)


      


@flow()
def etl_s3_to_rds():
    """Main ETL flow to load data into Big Query"""
    color="yellow"
    year=2021
    month=1

    load_dotenv()

    host = os.getenv('HOST')
    port = os.getenv('PORT')
    database = os.getenv('DATABASE')
    user = os.getenv('REDSHIFT_USER')
    password = os.getenv('REDSHIFT_PASSWORD')

    print(host, port, database, user, password)

    data = extract_from_s3(color, year, month)

    df = transform(data)

    
    write_rds(df, host, port, database, user, password)


if __name__ == "__main__":
    etl_s3_to_rds()