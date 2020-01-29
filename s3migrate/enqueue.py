#!/usr/bin/env python

import os
import sys
from multiprocessing.dummy import Pool as ThreadPool
from shared import chunked
import boto3
from tqdm import tqdm

from shared import make_generator_from_args

CLIENT = boto3.client('sqs', region_name=os.environ["AWS_S3_REGION_NAME"])
QUEUE_URL = os.environ["SQS_URL"]
THREADS = 1000

VERIFY = {'action': {'StringValue': 'verify', 'DataType': 'String'}}
MIGRATE = {'action': {'StringValue': 'migrate', 'DataType': 'String'}}


def send_message(mogile_file_list, action):
    """Send the mogile file as an SQS message."""
    entries = [{'Id': str(mogile_file.fid),
                'MessageAttributes': action,
                'MessageBody': mogile_file.to_json()}
               for mogile_file in mogile_file_list]
    CLIENT.send_message_batch(
        QueueUrl=QUEUE_URL,
        Entries=entries)


def queue_migrate(mogile_file_list):
    """Queue mogile files for migration in SQS."""
    send_message(mogile_file_list, MIGRATE)


def queue_verify(mogile_file_list):
    """Queue mogile files for verification in SQS."""
    send_message(mogile_file_list, VERIFY)


def main():
    """Enqueue mogile file jobs to SQS for processing in AWS lambda."""
    generator = chunked(tqdm(make_generator_from_args(sys.argv[2:])), size=10)
    pool = ThreadPool(THREADS)
    if sys.argv[1] == 'verify':
        pool.imap_unordered(queue_verify, generator)
    else:
        pool.imap_unordered(queue_migrate, generator)
    pool.close()
    pool.join()


if __name__ == "__main__":
    main()
