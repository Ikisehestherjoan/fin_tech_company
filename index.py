import boto3
import psycopg2
import pandas as pd
from sqlalchemy import create_engine
from configparser import ConfigParser 
from utils.helper import create_bucket 

config=ConfigParser()
config.read('.env')

access_key=config['AWS']['access_key']
secret_access_key=config['AWS']['secret_access_key']
region =config['AWS']['region']
bucket_name=config['AWS']['bucket_name']

host=config['DB_CRED']['host']
user=config['DB_CRED']['user']
password=config['DB_CRED']['password']
database=config['DB_CRED']['database']

#STEP 1 CREATE A BUCKET USING BOTO 3
create_bucket()


#step2 : extract from database to datalake

conn =create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:5432/{database}')
tables=['banks','cards','cust_verification_status','transaction_status','transactions','users','wallets']
s3_path='s3://{}/{}.csv'

for table in tables:
    query=f'SELECT * FROM {table}' 
    df=pd.read_sql_query(query,conn)


    df.to_csv(
    s3_path.format(bucket_name,table),
    index=False,
    storage_options={
        'key':access_key,
        'secret':secret_access_key
    }    
    )
    

