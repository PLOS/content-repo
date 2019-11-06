#!/usr/bin/env python

import base64
import hashlib
import os
import random
import shutil
import tempfile
import threading
from queue import Empty, Queue

import boto3
import dj_database_url
import pymogilefs
import pymysql
import requests
from botocore.exceptions import ClientError


def hash_fileobj(fileobj, hasher):
    """Efficiently hash a file object using the provided hasher."""
    blocksize = 65536
    fileobj.seek(0)
    block = fileobj.read(blocksize)
    while block:
        hasher.update(block)
        block = fileobj.read(blocksize)
    fileobj.seek(0)
    return hasher


def md5_fileobj_hex(fileobj):
    """Efficiently calculate the MD5 sum for a file object. Returns a hex
string."""
    return hash_fileobj(fileobj, hashlib.md5()).hexdigest()


def md5_fileobj_b64(fileobj):
    """Efficiently calculate the MD5 sum for a file object. Returns a
Base64 encoded string."""
    return base64.b64encode(
        hash_fileobj(fileobj, hashlib.md5()).digest()
    ).decode("utf-8")


def sha1_fileobj_hex(fileobj):
    """Efficiently calculate the SHA1 sum for a file object. Returns a hex
string."""
    return hash_fileobj(fileobj, hashlib.sha1()).hexdigest()


def sha1_fileobj_b64(fileobj):
    """Efficiently calculate the SHA1 sum for a file object. Returns a
Base64 encoded string."""
    return base64.b64encode(
        hash_fileobj(fileobj, hashlib.sha1()).digest()
    ).decode("utf-8")


class MyThread(threading.Thread):
    """Thread worker with s3 and mogile clients available."""

    def __init__(self, q: Queue, *args, **kwargs):
        super(MyThread, self).__init__(*args, **kwargs)
        self.queue = q
        self.mogile_client = pymogilefs.client.Client(
            trackers=os.environ['MOGILE_TRACKERS'].split(','),
            domain='plos_repo')
        self.s3_client = boto3.resource('s3')

    def run(self):
        while True:
            try:
                mogile_file = self.queue.get_nowait()
                print(f"Migrating {mogile_file.fid} to "
                      f"{mogile_file.make_contentrepo_key()}")
                mogile_file.migrate(
                    self.mogile_client,
                    self.s3_client,
                    os.environ['BUCKET'])
                self.queue.task_done()
            except Empty:
                break

    @classmethod
    def run_pool(cls, queue):
        """Run these threads on the data provided in the queue."""
        threads = []
        for _ in range(20):
            thread = cls(queue)
            threads.append(thread)
            thread.start()

        # block until all tasks are done
        queue.join()

        # stop workers
        for thread in threads:
            thread.join()


def main():
    """Perform copy of content from mogile to S3."""
    config = dj_database_url.parse(os.environ['MOGILE_DATABASE_URL'])
    connection = pymysql.connect(
        host=config['HOST'],
        user=config['USER'],
        password=config['PASSWORD'],
        db=config['NAME'],
        cursorclass=pymysql.cursors.DictCursor)

    # Uncomment to enable boto debug logging
    # boto3.set_stream_logger(name='botocore')

    try:
        with connection.cursor() as cursor:
            # Read a single record
            sql = "SELECT * FROM file"
            cursor.execute(sql)
            queue = Queue()
            while True:
                rows = cursor.fetchmany(1000)
                if len(rows) == 0:
                    break
                for row in rows:
                    queue.put(MogileFile.parse_row(row))
            MyThread.run_pool(queue)
    finally:
        connection.close()


class MogileFile():
    """Represents a file stored in mogile."""

    def __init__(self, fid: int, dkey: str, length: int):
        self.length = length
        self.fid = fid
        self.dkey = dkey
        (self.sha1sum, self.orig_bucket) = dkey.split('-', 1)

    @classmethod
    def parse_row(cls, row: dict):
        """Factory method to take an row as returned from the database and
return a MogileFile."""
        # Sanity check, we only use one "domain" and one class
        assert row['dmid'] == 1, "Bad domain"
        assert row['classid'] == 0, "Bad class"
        return MogileFile(dkey=row['dkey'],
                          fid=row['fid'],
                          length=row['length'])

    def exists_in_bucket(self, client, bucket, key):
        """Return True if object exists in the bucket."""
        obj = client.Object(bucket, key)
        try:
            obj.load()
            if obj.content_length != self.length:
                return False
        except ClientError as ex:
            if ex.response['Error']['Code'] == "404":
                return False
            raise
        return True

    def make_intermediary_key(self):
        """Return the key to use for the intermediary, mogile-based storage of
this file in S3."""
        padded = "{:010d}".format(self.fid)
        return "{first}/{second}/{third}/{padded}.fid".format(
            first=padded[0:1],
            second=padded[1:4],
            third=padded[4:7],
            padded=padded)

    def make_contentrepo_key(self):
        """Return the key to use for the final storage of this file in S3."""
        return self.sha1sum

    def intermediary_exists_in_bucket(self, client, bucket):
        """Return True if the intermediary (mogile-style key) object exists in
the bucket."""
        return self.exists_in_bucket(client, bucket,
                                     self.make_intermediary_key())

    def contentrepo_file_exists_in_bucket(self, client, bucket):
        """Return True if the final contentrepo object exists in the bucket."""
        return self.exists_in_bucket(client, bucket,
                                     self.make_contentrepo_key())

    def get_mogile_url(self, client):
        """Get a URL from mogile that we can use to access this file."""
        return random.choice(
            list(client.get_paths(self.dkey).data['paths'].values()))

    def copy_from_intermediary(self, s3_client, bucket):
        """Copy content from the intermediary (mogile-style key) location to
its final location."""
        obj = s3_client.Object(
            bucket,
            self.make_contentrepo_key())
        obj.copy({
            'Bucket': bucket,
            'Key': self.make_intermediary_key()
        })

    def put(self, mogile_client, s3_client, bucket):
        """Put content from mogile to S3."""
        with requests.get(
                self.get_mogile_url(mogile_client), stream=True
        ) as req:
            req.raise_for_status()
            with tempfile.TemporaryFile() as tmp:
                req.raw.decode_content = True
                shutil.copyfileobj(req.raw, tmp)
                assert sha1_fileobj_hex(tmp) == self.sha1sum
                md5 = md5_fileobj_b64(tmp)
                tmp.seek(0)
                target = s3_client.Object(
                    bucket,
                    self.make_contentrepo_key())
                target.put(
                    Body=tmp,
                    ContentMD5=md5)

    def migrate(self, mogile_client, s3_client, bucket):
        """Migrate this mogile file contentrepo."""
        if self.contentrepo_file_exists_in_bucket(s3_client, bucket):
            pass  # Migration done!
        else:
            if self.intermediary_exists_in_bucket(s3_client, bucket):
                self.copy_from_intermediary(s3_client, bucket)
            else:
                # Nothing is on S3 yet, copy content directly to the
                # final location.
                self.put(mogile_client, s3_client, bucket)


if __name__ == "__main__":
    main()
