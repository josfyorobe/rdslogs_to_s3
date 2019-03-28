#
# Copyright 2015 Ryan Holland
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
#
# You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and limitations under the License.
#

import json
import config
import boto3, botocore

s3_client = boto3.client('s3', region_name=config.region)
rds_client = boto3.client('rds', region_name=config.region)


def lambda_handler(event, context):
    last_written_time = get_last_written_time(config.bucket_name, config.last_received_file)
    db_logs = get_db_logs(config.rds_instance_name, config.log_name_prefix, last_written_time)
    for log in db_logs[:config.log_count]:
        last_written_time = log['LastWritten']
        upload_db_log(config.rds_instance_name, log['LogFileName'], config.bucket_name, config.s3_bucket_prefix)
    update_last_written_time(config.bucket_name, config.last_received_file, last_written_time)


def get_last_written_time(bucket_name, last_received_file):
    first_run = False
    last_written_time = 0
    
    try:
        s3_response = s3_client.head_bucket(Bucket=bucket_name)
    except botocore.exceptions.ClientError as e:
        error_code = int(e.response['ResponseMetadata']['HTTPStatusCode'])
        if error_code == 404:
            raise Exception("Error: Bucket name provided not found")
        else:
            raise Exception("Error: Unable to access bucket name, error: " + e.response['Error']['Message'])
    else:
        try:
            s3_response = s3_client.get_object(Bucket=bucket_name, Key=last_received_file)
        except botocore.exceptions.ClientError as e:
            error_code = int(e.response['ResponseMetadata']['HTTPStatusCode'])
            if error_code == 404:
                print("It appears this is the first log import, all files will be retrieved from RDS")
                first_run = True
            else:
                raise e
    if first_run == False:
        last_written_time = int(s3_response['Body'].read())
        print 'Retrieving last_written_time value from file: {} in bucket: {}'.format(last_received_file, bucket_name)

    return last_written_time


def get_db_logs(rds_instance_name, log_name_prefix, last_written_time):
    print 'Retrieving logs from rds: {} later than last_written_time: {}'.format(rds_instance_name, last_written_time)
    db_logs = rds_client.describe_db_log_files(
        DBInstanceIdentifier=rds_instance_name,
        FilenameContains=log_name_prefix
    )
    db_logs = sorted(db_logs['DescribeDBLogFiles'], key=lambda x: x['LastWritten'])
    db_logs = [item for item in db_logs if int(item['LastWritten']) > last_written_time]
    return db_logs


def upload_db_log(rds_instance_name, log_file_name, bucket_name, s3_bucket_prefix):
    log_file = rds_client.download_db_log_file_portion(
        DBInstanceIdentifier=rds_instance_name,
        LogFileName=log_file_name,
        Marker='0'
    )
    log_file_data = log_file['LogFileData']
    while log_file['AdditionalDataPending']:
        log_file = rds_client.download_db_log_file_portion(
            DBInstanceIdentifier=rds_instance_name,
            LogFileName=log_file_name,
            Marker=log_file['Marker'])
        log_file_data += log_file['LogFileData']
    byte_data = log_file_data.encode('utf-8')
    try:
        object_name = s3_bucket_prefix + log_file_name
        
        s3_response = s3_client.put_object(
            Bucket=bucket_name,
            Key=object_name,
            Body=byte_data
        )
    except botocore.exceptions.ClientError as e:
        return "Error writing object to S3 bucket, S3 ClientError: " + e.response['Error']['Message']
    print("Writing log file %s to S3 bucket %s" % (object_name, bucket_name))


def update_last_written_time(bucket_name, last_received_file, body):
    try:
        s3_response = s3_client.put_object(
            Bucket=bucket_name,
            Key=last_received_file,
            Body=str.encode(str(body))
        )
    except botocore.exceptions.ClientError as e:
        return "Error writing object to S3 bucket, S3 ClientError: " + e.response['Error']['Message']

    print("Wrote new Last Written Marker to %s in Bucket %s" % (last_received_file, bucket_name))

