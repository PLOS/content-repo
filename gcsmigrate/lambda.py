import os

import boto3
import pymogilefs

from shared import MogileFile, make_bucket_map
from google.cloud import storage

bucket_map = make_bucket_map(os.environ["BUCKETS"])
mogile_client = pymogilefs.client.Client(
    trackers=os.environ['MOGILE_TRACKERS'].split(','),
    domain='plos_repo')
table = boto3.resource('dynamodb').Table(os.environ["DYNAMODB_TABLE"])
gcs_client = storage.Client()

def process(event, _context):
    """Migrate this MogileFile to GCS in AWS Lambda."""
    for record in event['Records']:
        action = record["messageAttributes"]["action"]["stringValue"]
        mogile_file = MogileFile.from_json(record["body"])

        if action == "verify":
            response = table.get_item(Key={'fid': mogile_file.fid})
            assert "Item" in response, f"No db entry for {mogile_file.fid}."
            item = response["Item"]
            mogile_file.verify(
                sha1=item['sha1'],
                md5=item['md5'],
                fid=int(item['fid']),
                bucket=item['bucket'],
                bucket_map=bucket_map,
                gcs_client=gcs_client,
            )
        else:
            mogile_file.migrate(
                mogile_client,
                table,
                gcs_client,
                bucket_map)

