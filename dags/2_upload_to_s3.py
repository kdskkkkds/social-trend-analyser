def upload():
    import requests,json,random
    from datetime import datetime
    import pandas as pd
    import logging
    from configparser import ConfigParser
    import boto3
    import os

    parser = ConfigParser()
    parser.read('C:/Users/ZMO-WIN-KedarS-01/Desktop/sta_project/sta.properties')
    aws_access_key1 = parser.get('details', 'aws_access_key')
    aws_secret_access_key1 = parser.get('details', 'aws_secret_access_key')
    username = parser.get('details', 'username')
    password = parser.get('details', 'password')
    list1 = parser.get('details', 'list1')

    s3 = boto3.client('s3', aws_access_key_id=aws_access_key1,aws_secret_access_key=aws_secret_access_key1)
    s3.upload_file('user_info/user.csv', 'sta-pro-l0-prod', '1.0/L0/user.csv')
    s3.upload_file('user_acc_info/user_account.csv', 'sta-pro-l0-prod', '1.0/L0/user_account.csv')
    s3.upload_file('user_post_info/account_post.csv', 'sta-pro-l0-prod', '1.0/L0/account_post.csv')
    os.remove('user_info/user.csv')
    os.remove('user_acc_info/user_account.csv')
    os.remove('user_post_info/account_post.csv')