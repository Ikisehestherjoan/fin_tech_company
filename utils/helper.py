import boto3
access_key='AKIAWYIBM2R4FUNNLREB'
secret_access_key='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'
region ='eu-west-1' 
bucket_name='bibipayminute'  

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