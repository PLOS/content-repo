#!/usr/bin/env python

import os
import threading
from queue import Empty, Queue

import boto3
import pymogilefs

from shared import make_bucket_map, get_mogile_files_from_database


class MyThread(threading.Thread):
    """Thread worker with s3 and mogile clients available."""

    def __init__(self, queue: Queue, bucket_map: dict, *args, **kwargs):
        """Initialize this thread."""
        super(MyThread, self).__init__(*args, **kwargs)
        self.queue = queue
        self.bucket_map = bucket_map
        self.mogile_client = pymogilefs.client.Client(
            trackers=os.environ['MOGILE_TRACKERS'].split(','),
            domain='plos_repo')
        self.s3_client = boto3.resource('s3')

    def run(self):
        """Run this thread."""
        while True:
            try:
                mogile_file = self.queue.get_nowait()
                s3_bucket = self.bucket_map[mogile_file.mogile_bucket]
                mogile_file.migrate(
                    self.mogile_client,
                    self.s3_client,
                    s3_bucket)
                self.queue.task_done()
            except Empty:
                break

    @classmethod
    def start_pool(cls, queue: Queue, bucket_map: dict):
        """Run threads on the data provided in the queue.

        Returns a list of threads to pass to `finish_pool` later.
        """
        threads = []
        for _ in range(20):
            thread = cls(queue, bucket_map)
            threads.append(thread)
            thread.start()
        return threads

    @staticmethod
    def finish_pool(queue, threads):
        """Wait until queue is empty and threads have finished processing."""
        # block until all tasks are done
        queue.join()

        # stop workers
        for thread in threads:
            thread.join()


def main():
    """Perform copy of content from mogile to S3."""
    # Uncomment to enable boto debug logging
    # boto3.set_stream_logger(name='botocore')

    bucket_map = make_bucket_map(os.environ["BUCKETS"])
    queue = Queue()
    counter = 0
    threads = None
    for mogile_file in get_mogile_files_from_database(
            os.environ['MOGILE_DATABASE_URL']):
        queue.put(mogile_file)
        counter = counter + 1
        if counter == 1000:
            # Start up the consumer threads once we have 1000 entries
            threads = MyThread.start_pool(queue, bucket_map)
    if threads is None:
        # In case we did not get 1000 items
        threads = MyThread.start_pool(queue, bucket_map)
    MyThread.finish_pool(queue, threads)


if __name__ == "__main__":
    main()
