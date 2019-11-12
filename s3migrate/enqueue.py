#!/usr/bin/env python

import os

import boto3

from shared import get_mogile_files_from_database

QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/429579970117/mogile-to-s3'


def main():
    """Enqueue mogile files to SQS."""
    client = boto3.client('sqs', region_name=os.environ["AWS_S3_REGION_NAME"])
    for mogile_file in get_mogile_files_from_database(
            os.environ['MOGILE_DATABASE_URL']):
        client.send_message(
            QueueUrl=QUEUE_URL,
            MessageBody=mogile_file.to_json())


if __name__ == "__main__":
    main()
