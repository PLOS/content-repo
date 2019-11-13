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
    s3_client = boto3.resource('s3')
    dynamodb = boto3.resource('dynamodb')

    for record in event['Records']:
        mogile_file = MogileFile.from_json(record["body"])
        s3_bucket = bucket_map[mogile_file.mogile_bucket]
        md5 = mogile_file.migrate(
            mogile_client,
            s3_client,
            s3_bucket)
        table = dynamodb.Table(os.environ["DYNAMODB_TABLE"])
        table.put_item(
            Item={
                'fid': mogile_file.fid,
                'sha1': mogile_file.sha1sum,
                'md5': md5,
                'bucket': s3_bucket
            })
