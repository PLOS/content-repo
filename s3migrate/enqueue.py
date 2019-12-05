#!/usr/bin/env python

import os
import sys
from multiprocessing.dummy import Pool as ThreadPool
from shared import chunked
import boto3

from shared import get_mogile_files_from_database, \
    make_generator_from_args

CLIENT = boto3.client('sqs', region_name=os.environ["AWS_S3_REGION_NAME"])
QUEUE_URL = os.environ["SQS_URL"]
THREADS = 1000


def send_message(mogile_file_list):
    """Send the mogile file as an SQS message."""
    sys.stderr.write("*")
    sys.stderr.flush()
    entries = [{'Id': str(mogile_file.fid),
                'MessageBody': mogile_file.to_json()}
               for mogile_file in mogile_file_list]
    CLIENT.send_message_batch(
        QueueUrl=QUEUE_URL,
        Entries=entries)


def main():
    """Enqueue mogile files to SQS."""
    generator = chunked(make_generator_from_args(sys.argv), size=10)
    pool = ThreadPool(THREADS)
    pool.imap_unordered(send_message, generator)
    pool.close()
    pool.join()


if __name__ == "__main__":
    main()
