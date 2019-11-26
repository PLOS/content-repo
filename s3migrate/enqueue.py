#!/usr/bin/env python

import os
import sys
from multiprocessing.dummy import Pool as ThreadPool

import boto3

from shared import get_mogile_files_from_database

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


def chunked(iterable):
    """Group into chunks of 10."""
    result = []
    for item in iterable:
        result.append(item)
        if len(result) == 10:
            yield result
            result = []
    if len(result) > 0:
        yield result


def main():
    """Enqueue mogile files to SQS."""
    excluded_fids = set()
    if len(sys.argv) > 1:
        with open(sys.argv[1]) as f:
            for line in f:
                excluded_fids.add(int(line))
        print(f"Excluding {len(excluded_fids)} fids.")
    generator = chunked(
        get_mogile_files_from_database(
            os.environ['MOGILE_DATABASE_URL'],
            excluded_fids=excluded_fids))
    pool = ThreadPool(THREADS)
    pool.imap_unordered(send_message, generator)
    pool.close()
    pool.join()


if __name__ == "__main__":
    main()
