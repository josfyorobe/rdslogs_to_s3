import os

bucket_name = os.environ['BucketName']
log_count = int(os.environ['LogCount'])
log_name_prefix = os.environ['LogNamePrefix']
rds_instance_name = os.environ['RDSInstanceName']
region = os.environ['Region']
s3_bucket_prefix = os.environ['S3BucketPrefix']
last_received_file = os.environ['lastReceivedFile']
