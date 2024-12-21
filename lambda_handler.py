import io
import json

import boto3
import pandas as pd


def lambda_handler(event, context):
    # TODO implement
    print(event)
    
    bucket_name=event['Records'][0]['s3']['bucket']['name']
    file_name=event['Records'][0]['s3']['object']['key']
    
    print(bucket_name,file_name)
    
    s3 = boto3.client('s3') 
    obj = s3.get_object(Bucket= bucket_name, Key= file_name) 
    csv_data=obj['Body'].read().decode('utf-8')
    
    df = pd.read_csv(io.StringIO(csv_data))
    print(df.to_string())
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
