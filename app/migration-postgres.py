import pandas as pd
import sys
from sqlalchemy import create_engine
from time import time
import argparse
import os


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db


    # ESTABLISHING POSTGRES DATABASE CONNECTION
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # READING IN LOCAL PARQUET FILE
    fighters = pd.read_csv("./static/csvfiles/fighters.csv")
    stats = pd.read_csv("./static/csvfiles/stats.csv")

    # INITIALIZING CHUNKSIZE AND TIME ELAPSED VARIABLES
    chunksize = 10000
    time_elapsed = 0
    
    # LOADING DATA CHUNKS OF SIZE 10000 FROM DF TO POSTGRES DATABASE
    print('Data Migration in process...')
    start = time()
    fighters.to_sql("fighters", con=engine, if_exists = 'replace', chunksize=chunksize)
    stats.to_sql("stats", con=engine, if_exists = 'replace', chunksize=chunksize)
    end = time()
    
    # PRINT SMALL REPORT NOTING HOW LONG THE EXECUTION OF THE SCRIPT TOOK
    time_elapsed = end - start
    print(f'Data loaded into Postgres, {len(fighters) + len(stats)} records took {time_elapsed} seconds.')
    


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    # user
    # password
    # host
    # port
    # database name
    # table name

    for arg in ['--user', '--password', '--host', '--port', '--db']:
        parser.add_argument(arg, f'{arg} for Postgres') 

    args = parser.parse_args()

    main(args)