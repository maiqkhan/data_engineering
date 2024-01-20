import pandas as pd 
from sqlalchemy import create_engine
import argparse
import os




def main(params):
    user = params.user 
    password = params.password 
    host = params.host 
    port = params.port 
    db = params.db 
    table_name = params.table_name
    url = params.url 

    csv_name = 'output.csv'

    os.system(f"wget {url} -O {csv_name}")

    print(csv_name)
    df = pd.read_csv(csv_name, compression='gzip')


    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df.head(0).to_sql(name=table_name, con=engine, if_exists='replace')

    df.to_sql(name=table_name, con=engine, if_exists='append', chunksize=10_000)
    print(f'Data from {csv_name} pushed to {table_name} table in Postgres')

if __name__ == "__main__":
    
    
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres.')
    parser.add_argument('--user')
    parser.add_argument('--password')
    parser.add_argument('--host')
    parser.add_argument('--port')
    parser.add_argument('--db')
    parser.add_argument('--table_name')
    parser.add_argument('--url')

    args = parser.parse_args()

    main(args)
