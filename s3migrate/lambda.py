import json
import os

import boto3
import pymogilefs

from shared import MogileFile, make_bucket_map


def process(event, _context):
    """Migrate this MogileFile to S3 in AWS Lambda."""
    bucket_map = make_bucket_map(os.environ["BUCKETS"])
    mogile_client = pymogilefs.client.Client(
        trackers=os.environ['MOGILE_TRACKERS'].split(','),
        domain='plos_repo')
    s3_resource = boto3.resource('s3')
    dynamodb = boto3.resource('dynamodb')

    for record in event['Records']:
        mogile_file = MogileFile.from_json(record["body"])
        mogile_file.migrate(
            mogile_client,
            dynamodb,
            s3_resource,
            bucket_map)
