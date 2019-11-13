#!/usr/bin/env python

import os
import sys
from multiprocessing.dummy import Pool as ThreadPool

import boto3

from shared import get_mogile_files_from_database

CLIENT = boto3.client('sqs', region_name=os.environ["AWS_S3_REGION_NAME"])
QUEUE_URL = os.environ["SQS_URL"]
THREADS = 100


def send_message(mogile_file):
    """Send the mogile file as an SQS message."""
    sys.stderr.write("*")
    sys.stderr.flush()
    CLIENT.send_message(
        QueueUrl=QUEUE_URL,
        MessageBody=mogile_file.to_json())


def main():
    """Enqueue mogile files to SQS."""
    generator = get_mogile_files_from_database(
        os.environ['MOGILE_DATABASE_URL'])
    pool = ThreadPool(THREADS)
    pool.map(send_message, generator)
    pool.close()
    pool.join()


if __name__ == "__main__":
    main()
