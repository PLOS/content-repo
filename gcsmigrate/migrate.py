#!/usr/bin/env python

import os
import sys
from queue import Queue

import boto3
import pymogilefs
from google.cloud import storage

from shared import make_bucket_map, \
    make_generator_from_args, QueueWorkerThread


class MyThread(QueueWorkerThread):
    """Thread worker with GCS and mogile clients available."""

    def __init__(self, queue: Queue, *args, **kwargs):
        """Initialize this thread."""
        super().__init__(queue, *args, **kwargs)
        self.mogile_client = pymogilefs.client.Client(
            trackers=os.environ['MOGILE_TRACKERS'].split(','),
            domain='plos_repo')
        self.gcs_client = storage.Client()
        self.table = boto3.resource('dynamodb').Table(os.environ["DYNAMODB_TABLE"])
        self.bucket_map = make_bucket_map(os.environ["BUCKETS"])

    def dowork(self, mogile_file):
        """Perform the migration."""
        mogile_file.migrate(
            self.mogile_client,
            self.table,
            self.gcs_client,
            self.bucket_map)


def main():
    """Perform copy of content from mogile to S3."""
    # Uncomment to enable boto debug logging
    # boto3.set_stream_logger(name='botocore')
    generator = make_generator_from_args(sys.argv[1:])
    MyThread.process_generator(20, generator)


if __name__ == "__main__":
    main()
