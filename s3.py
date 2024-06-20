import smtplib
import logging
from logging.handlers import RotatingFileHandler
from email.mime.text import MIMEText
from pathlib import Path
import logging_loki
import configparser
import psycopg2
import hashlib
import time, datetime
from time import gmtime, strftime
import os
from urllib import request, parse, error
import json
from datetime import datetime,timezone
from dateutil.relativedelta import relativedelta 
import rfc3339      # for date object -> date string
import pytz
import common
from email.mime.multipart import MIMEMultipart 
from email.mime.text import MIMEText 
from email.mime.application import MIMEApplication
import logging
import boto3
import requests
from botocore.exceptions import ClientError
import base64
import json
import hmac
import hashlib
import os


def put_file_objectstorage(config_section):
        user,password,host,port,database,connect_timeout,backup_path,instance_id,url, auth_token,region=common.read_config_flatten(config_section)
        latz = common.set_timezone("America/Los_Angeles")
        mydate = datetime.now(latz).replace(day=datetime.now().day + 1)   
        iso_date= mydate.replace(microsecond=0).isoformat()
        iso_date = common.get_date_string(mydate)
        print ("runnning backup expires at {}".format(iso_date))
        data=json.dumps({"database_name": "{}".format(database),
                        "instance_id": "{}".format(instance_id),
                        "name": "Backup_End_Of_Month",
                        "expires_at":"{}".format(iso_date)
                        }).encode('ascii')
        req = request.Request(url, data=data,  method="POST")
        req.add_header('Content-Type', 'application/json')
        req.add_header('X-Auth-Token', auth_token)
        # Sending request to Instance API
        try:
            res=request.urlopen(req).read().decode()
        except error.HTTPError as e:
            res=e.read().decode()
            print (res)
            common.logging_info(str(datetime.now()) + "   res")
        return {
            "body": json.loads(res),
            "headers": {
            "Content-Type": ["application/json"],
            },
            "statusCode": 200,
        }
        try:
            res = request.urlopen(req).read().decode()
        except error.HTTPError as e:
            res = e.read().decode()
            return {
            "body": json.loads(res),
            "headers": {
            "Content-Type": ["application/json"],
                    },
        "statusCode": 200,
            }
        
def put_object(config_section):
    user,password,host,port,database,connect_timeout,backup_path,instance_id,url, auth_token,region=common.read_config_flatten(config_section)

    # Generate your access key from the console
    ACCESS_KEY_ID = "SCWN0TDSPNJE8BQA05AW"
    SECRET_ACCESS_KEY = "544d8550-24af-4987-b8bc-9e8dbd79eaaa"
    # S3 Region
    REGION = "fr-par"
    # Example for the demo
    DATE = "20240412"
    X_AMZ_DATE = "20240412T000000Z"
    EXPIRATION = "2024-04-13T02:00:00.000Z"
    policy = {
        "expiration": EXPIRATION,
        "conditions": [
            {"bucket":"my-bucket"},
            ["starts-with","$key",""],
            {"acl":"public-read"},
            {"x-amz-credential": ACCESS_KEY_ID + "/" + DATE + "/" + REGION + "/s3/aws4_request"},
            {"x-amz-algorithm":"AWS4-HMAC-SHA256"},
            {"x-amz-date": X_AMZ_DATE},
            {"success_action_status":"204"}
        ]
    }
    stringToSign = base64.b64encode(bytes(json.dumps(policy), encoding='utf8'))
    print("Base64 encoded policy:", stringToSign.decode("utf-8"), end="\n\n")
    # Base64 encoded policy: eyJleHBpcmF0aW9uIjogIjIwMTktMDktMTlUMDI6MDA6MDAuMDAwWiIsICJjb25kaXRpb25zIjogW3siYnVja2V0IjogIm15YnVja2V0In0sIFsic3RhcnRzLXdpdGgiLCAiJGtleSIsICIiXSwgeyJhY2wiOiAicHVibGljLXJlYWQifSwgeyJ4LWFtei1jcmVkZW50aWFsIjogIlNDV1hYWFhYWFhYWFhYWFhYWFhYLzIwMTkwOTE4L2ZyLXBhci9zMy9hd3M0X3JlcXVlc3QifSwgeyJ4LWFtei1hbGdvcml0aG0iOiAiQVdTNC1ITUFDLVNIQTI1NiJ9LCB7IngtYW16LWRhdGUiOiAiMjAxOTA5MTlUMDAwMDAwWiJ9LCB7InN1Y2Nlc3NfYWN0aW9uX3N0YXR1cyI6ICIyMDQifV19
    dateKey = hmac.new(bytes("AWS4" + SECRET_ACCESS_KEY, 'utf-8'), bytes(DATE, 'utf-8'), digestmod=hashlib.sha256).digest()
    dateRegionKey = hmac.new(dateKey, bytes(REGION, 'utf-8'), digestmod=hashlib.sha256).digest()
    dateRegionServiceKey = hmac.new(dateRegionKey, bytes("s3", 'utf-8'), digestmod=hashlib.sha256).digest()
    signinKey = hmac.new(dateRegionServiceKey, bytes("aws4_request", 'utf-8'), digestmod=hashlib.sha256).digest()
    print("Signin key:", signinKey.hex(), end="\n\n")
    # Signin key: 9c3ad81294a9263e472165307ae6e5c64e738b6837ff688f6e721a5ed53a4873
    signature = hmac.new(signinKey, stringToSign, digestmod=hashlib.sha256).digest()
    print("Signature:", signature.hex(), end="\n\n")
    # Signature: 4d879d70f91e6f2f417163b4a1e90e0e6b32c99cbe3eaa168bc7ab216d33d784

    # Generate a presigned URL for the S3 object
    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        region_name='fr-par',
        use_ssl=True,
        endpoint_url='https://dbbackup01.s3.fr-par.scw.cloud',
        aws_access_key_id='SCWN0TDSPNJE8BQA05AW',
        aws_secret_access_key='544d8550-24af-4987-b8bc-9e8dbd79eaaa'
    )
    bucket_name = "dbbackup01"
    object_name = "${filename}"
    fields = {
            "acl": "public-read",
            "Cache-Control": "nocache",
            "Content-Type": "text"
        }
    conditions = [
        {"key": "my-object"},
        {"acl": "public-read"},
        {"Cache-Control": "nocache"},
        {"Content-Type": "text"}
    ]
    expiration = 120 # Max two minutes to start upload
    try:
        response = s3_client.generate_presigned_post(Bucket=bucket_name,
                                                        Key=object_name,
                                                        Fields=fields,
                                                        Conditions=conditions,
                                                        ExpiresIn=expiration)
    except ClientError as e:
        logging.error(e)
        exit()
    # The response contains the presigned URL and required fields
    print(response)
    with open('requirements.txt', 'rb') as f:
        files = {'file': (object_name, f)}
        print(files)
        http_response = requests.post(response['url'], data=response['fields'], files=files)
        print(http_response.content)


# scw object config get type=s3cmd
# # Generated by scaleway-cli command
# # Configuration file for s3cmd https://s3tools.org/s3cmd
# # Default location: $HOME/.s3cfg
# [default]
# access_key = ****
# bucket_location = fr-par
# host_base = s3.fr-par.scw.cloud
# host_bucket = %(bucket)s.s3.fr-par.scw.cloud
# secret_key = e
# use_https = True
# export AWS_ACCESS_KEY_ID="***"
# export AWS_SECRET_ACCESS_KEY=****
# export S3_ENDPOINT=https://s3.fr-par.scw.cloud
# export S3_REGION=fr-par

# function to upload file to S3 blob storage Scalaway
def upload_file_to_s3(file_path, bucket_name, object_name):
    # Get the access key and secret key from environment variables
    aws_access_key_id='SCWN0TDSPNJE8BQA05AW',
    aws_secret_access_key='544d8550-24af-4987-b8bc-9e8dbd79eaaa'
    access_key = os.environ.get('AWS_ACCESS_KEY_ID')
    access_key= aws_access_key_id
    secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
    secret_key=aws_secret_access_key
    # Get the S3 endpoint from environment variable
    s3_endpoint = os.environ.get('S3_ENDPOINT')
    s3_endpoint= "https://s3.fr-par.scw.cloud"
    s3_region="fr-par"

    # Get the S3 region from environment variable
    s3_region = os.environ.get('S3_REGION')
    s3_region='fr-par'
    # Check if the access key and secret key are available
    if s3_region and s3_endpoint and access_key and secret_key:
        # Create an S3 client with the credentials
        s3 = boto3.client('s3', region_name=s3_region, endpoint_url=s3_endpoint, aws_access_key_id=access_key, aws_secret_access_key=secret_key)
        try:
            s3.upload_file(file_path, bucket_name, object_name)
            print(f"File uploaded successfully to S3 bucket: {bucket_name}")
        except Exception as e:
            print(f"Error uploading file to S3: {e}")
            exit -1;
    else:
        print("Error: AWS credentials not found in environment variables.")
        exit -2;

if  __name__ == '__main__' :
    file="/root/gp/backup/db_short_202406190800.dmp"
    bucket='dbbackup01'
    target_name = 'Nico/toto.exe'
    upload_file_to_s3(file,bucket, target_name)