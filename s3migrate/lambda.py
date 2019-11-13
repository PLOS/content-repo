import json
import os
from shared import MogileFile, make_bucket_map
import pymogilefs
import boto3

def process(event, context):
    """Migrate this MogileFile to S3 in AWS Lambda."""
    bucket_map = make_bucket_map(os.environ["BUCKETS"])
    mogile_client = pymogilefs.client.Client(
        trackers=os.environ['MOGILE_TRACKERS'].split(','),
        domain='plos_repo')
    s3_client = boto3.resource('s3')

    for record in event['Records']:
        mogile_file = MogileFile.from_json(record["body"])
        mogile_file.migrate(
            mogile_client,
            s3_client,
            bucket_map)

