from pathlib import Path 
import pandas as pd 
from datetime import timedelta
from prefect import flow, task 
from prefect.tasks import task_input_hash
from prefect_aws import AwsCredentials
from prefect_aws.s3 import s3_upload
from prefect_aws.s3 import S3Bucket



@task(retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def fetch(url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas Dataframe"""

    if url[-2:] == 'gz':
        df = pd.read_csv(url, compression='gzip')
    else:
        df = pd.read_csv(url)

    return df

@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])

    return df

@task(log_prints=True)
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DataFrame out as parquet file"""
    path = Path(f"data/{color}/").resolve()
    
    path.mkdir(parents=True, exist_ok=True)
    
    df.to_parquet(path / f"{dataset_file}.parquet", compression='gzip', index=False)

    return Path(f"data/{color}/{dataset_file}.parquet").as_posix()

@flow(log_prints=True)
async def write_s3(path: Path) -> None:
    
    aws_credentials_block = await AwsCredentials.load("aws-credentials")

    credentials = AwsCredentials(
        aws_access_key_id=aws_credentials_block.aws_access_key_id,
        aws_secret_access_key=aws_credentials_block.aws_secret_access_key,
    )

    with open(path, "rb") as file:
        key = await s3_upload(data = file.read(),
                    bucket=f'mage-demo-bucket-mkhan',
                    aws_credentials=credentials,
                    key=f"{path}", )


@flow(name='main etl flow')
def etl_web_to_s3() -> None:
    """The main ETL function"""
    color = 'yellow'
    year = 2021 
    month = 1
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)

    df_clean = clean(df)

    path = write_local(df_clean, color, dataset_file)
    write_s3(path)

if __name__ == "__main__":
    etl_web_to_s3()