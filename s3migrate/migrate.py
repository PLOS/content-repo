#!/usr/bin/env python

import os
import threading
from queue import Empty, Queue

import boto3
import dj_database_url
import pymogilefs
import pymysql

from shared import make_bucket_map, MogileFile


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
                mogile_file.migrate(
                    self.mogile_client,
                    self.s3_client,
                    self.bucket_map)
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
    bucket_map = make_bucket_map(os.environ["BUCKETS"])
    config = dj_database_url.parse(os.environ['MOGILE_DATABASE_URL'])
    connection = pymysql.connect(
        host=config['HOST'],
        user=config['USER'],
        password=config['PASSWORD'],
        db=config['NAME'],
        cursorclass=pymysql.cursors.SSDictCursor)

    # Uncomment to enable boto debug logging
    # boto3.set_stream_logger(name='botocore')

    try:
        with connection.cursor() as cursor:
            row_count = 1000
            sql = "SELECT * FROM file"
            cursor.execute(sql)
            queue = Queue()
            # Ensure the queue has some content for the threads to consume.
            for row in cursor.fetchmany(row_count):
                queue.put(MogileFile.parse_row(row))
            threads = MyThread.start_pool(queue, bucket_map)
            rows = cursor.fetchmany(row_count)
            while len(rows) != 0:
                for row in rows:
                    queue.put(MogileFile.parse_row(row))
                rows = cursor.fetchmany(row_count)
            MyThread.finish_pool(queue, threads)
    finally:
        connection.close()


if __name__ == "__main__":
    main()
