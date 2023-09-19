import boto3
from configparser import ConfigParser 
from utils.helper import create_bucket 

config=ConfigParser()
config.read('.env')

access_key=config['AWS']['access_key']
secret_access_key=config['AWS']['secret_access_key']
region =config['AWS']['region']
bucket_name=config['AWS']['bucket_name']


def create_bucket():
    client = boto3.client(
        's3',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_access_key

    )

    client.create_bucket(
        Bucket=bucket_name,
        CreateBucketConfiguration={
            'LocationConstraint':region
        }
    ) 
