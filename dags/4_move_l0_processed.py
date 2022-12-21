def l0_processed():
    import requests,json,random
    from datetime import datetime
    import pandas as pd
    import logging
    from configparser import ConfigParser
    import boto3

    parser = ConfigParser()
    parser.read('C:/Users/ZMO-WIN-KedarS-01/Desktop/sta_project/sta.properties')
    aws_access_key1 = parser.get('details', 'aws_access_key')
    aws_secret_access_key1 = parser.get('details', 'aws_secret_access_key')

    ts = datetime.now().timestamp()

    s3 = boto3.client('s3',aws_access_key_id=aws_access_key1,aws_secret_access_key=aws_secret_access_key1)

    response1 = s3.get_object(Bucket='sta-pro-l0-prod',Key='1.0/L0/user.csv')
    content1 = response1['Body'].read ().decode ('utf-8')
    s3.put_object(Bucket='sta-pro-l0-prod',Body= content1,Key=f'1.0/processed/user_{ts}.csv')
    s3.delect_object(Bucket='sta-pro-l0-prod',Key='1.0/L0/user.csv')

    response2 = s3.get_object(Bucket='sta-pro-l0-prod',Key='1.0/L0/user_account.csv')
    content2 = response2['Body'].read ().decode ('utf-8')
    s3.put_object(Bucket='sta-pro-l0-prod',Body= content2,Key=f'1.0/processed/user_account_{ts}.csv')
    s3.delect_object(Bucket='sta-pro-l0-prod',Key='1.0/L0/user_account.csv')

    response3 = s3.get_object(Bucket='sta-pro-l0-prod',Key='1.0/L0/account_post.csv')
    content3 = response3['Body'].read ().decode ('utf-8')
    s3.put_object(Bucket='sta-pro-l0-prod',Body= content3,Key=f'1.0/processed/account_post_{ts}.csv')
    s3.delect_object(Bucket='sta-pro-l0-prod',Key='1.0/L0/account_post.csv')