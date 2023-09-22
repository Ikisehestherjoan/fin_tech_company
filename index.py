import boto3
import psycopg2
import pandas as pd
from sqlalchemy import create_engine
import redshift_connector
from configparser import ConfigParser 
from utils.helper import create_bucket 
import logging
from sql_statements.create import dev_tables,transformed_tables
from sql_statements.transform import transformation_queries

config=ConfigParser()
config.read('.env')

access_key=config['AWS']['access_key']
secret_access_key=config['AWS']['secret_access_key']
region =config['AWS']['region']
role =config['AWS']['arn']
bucket_name=config['AWS']['bucket_name']

db_host=config['DB_CRED']['host']
db_user=config['DB_CRED']['user']
db_password=config['DB_CRED']['password']
db_database=config['DB_CRED']['database']


dwh_host=config['DWH_CONN']['host']
dwh_user=config['DWH_CONN']['user']
dwh_password=config['DWH_CONN']['password']
dwh_database=config['DWH_CONN']['database']

#STEP 1 CREATE A BUCKET USING BOTO 3
create_bucket()


#step2 : extract from database to datalake

# conn =create_engine(f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:5432/{db_database}')
db_tables=['banks','cards','cust_verification_status','transaction_status','transactions','users','wallets']
# s3_path='s3://{}/{}.csv'

for table in db_tables:
    query=f'SELECT * FROM {table}'
    logging.info(f'=============== Executing {query}') 
    df=pd.read_sql_query(query,conn)


    df.to_csv(
    s3_path.format(bucket_name,table),
    index=False,
    storage_options={
        'key':access_key,
        'secret':secret_access_key
    }    
    )
    
#     #step 3 : load into schema
# # Connects to Redshift cluster using AWS credentials
dwh_conn = redshift_connector.connect(
    host=dwh_host,
    database=dwh_database,
    user=dwh_user,
    password=dwh_password
 )
# print('DWH ESTABLISH')
cursor=dwh_conn.connect()

dev_schema='dev'
staging_schema='staging'
#     #--creating the dev schema
cursor.execute(f''' CREATE SCHEMA {dev_schema} ''')
dwh_conn.commit()
#CREATE THE TABLES
for query in dev_tables:
    print('------------------{query[:50]}')
    cursor.exceute(query)
    dwh_conn.commit()


# #COPY TABLES FROM S3
for table in db_tables:
    cursor.execute(f'''
 COPY {dev_schema}.{table} 
FROM ''s3://{bucket_name}/{table}.csv''
IAM_ROLE '{role}'
DELIMETER ','
 IGNOREHEADER 1;
''')

dwh_conn.commit()


# STEP 4:  create facts and dimensions
#--- create schema

cursor.exceute(f'''
        CREATE SCHEMA {staging_schema};
''')
dwh_conn.commit()
for query in transformed_tables:
    cursor.execute(query)
    dwh_conn.commit()


#----insert the data into the facts and dimensions
for query in transformation_queries:
    cursor.execute(query)
    dwh_conn.commit()
    


# ---STEP 5: DATA QUALITY CHECK
staging_tables=['dim_customers','dim_banks','dim_dates','dim_wallets','ft_customers_transactions']
query='SELECT COUNT(*) FROM staging.{}'

for table in staging_tables:
    cursor.execute(query.format(table))
    print(f'Table {table} has {cursor.fetchall()}')

cursor.close()
dwh_conn.close()

