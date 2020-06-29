"""Tools for migrating Mogile content to S3."""

import base64
import hashlib
import json
import os
import random
import shutil
import tempfile
import threading
from queue import Empty, Queue

import dj_database_url
import pymysql
import requests
from botocore.exceptions import ClientError

BUFSIZE = 16*1024*1024  # 16 MiB


def make_bucket_map(buckets):
    """Construct a hash from a buckets source string of the form a:b,c:d."""
    retval = {}
    for bucket in buckets.split(','):
        source, target = bucket.split(':')
        retval[source] = target
    return retval


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
    """Efficiently calculate the MD5 sum for a file object.

    Returns a hex string.
    """
    return hash_fileobj(fileobj, hashlib.md5()).hexdigest()


def md5_fileobj_b64(fileobj):
    """Efficiently calculate the MD5 sum for a file object.

    Returns a Base64 encoded string.
    """
    return base64.b64encode(
        hash_fileobj(fileobj, hashlib.md5()).digest()
    ).decode("utf-8")


def sha1_fileobj_hex(fileobj):
    """Efficiently calculate the SHA1 sum for a file object.

    Returns a hex string.
    """
    return hash_fileobj(fileobj, hashlib.sha1()).hexdigest()


def sha1_fileobj_b64(fileobj):
    """Efficiently calculate the SHA1 sum for a file object.

    Returns a Base64 encoded string.
    """
    return base64.b64encode(
        hash_fileobj(fileobj, hashlib.sha1()).digest()
    ).decode("utf-8")


class MogileFile():
    """Represents a file stored in mogile."""

    def __init__(self, fid: int, dkey: str, length: int):
        """Initialize a MogileFile.

        Arguments are as stored in the mogile `file` database table.
        """
        self.length = length
        self.fid = fid
        self.dkey = dkey
        if dkey[36:] == ".tmp":
            # These seem to be old junk leftover from failed ingests.
            # Check later.
            self.temp = True
            self.sha1sum = None
            self.mogile_bucket = None
        else:
            self.temp = False
            self.sha1sum = dkey[0:40]
            self.mogile_bucket = dkey[41:]
            assert len(self.sha1sum) == 40
            assert dkey[40] == "-"
            assert len(self.mogile_bucket) > 1

    def __eq__(self, other):
        """Equality check."""
        if not isinstance(other, MogileFile):
            # Delegate comparison to the other instance's __eq__.
            return NotImplemented
        return(self.dkey == other.dkey and
               self.fid == other.fid and
               self.length == other.length)

    @classmethod
    def parse_row(cls, row: dict):
        """Take a file row and return a MogileFile."""
        # (fid, dmid, dkey, length, classid, devcount)
        # Sanity check, we only use one "domain" and one class
        assert row[1] == 1, "Bad domain"
        assert row[4] == 0, "Bad class"
        return cls(dkey=row[2],
                   fid=row[0],
                   length=row[3])

    def exists_in_bucket(self, client, s3_bucket, key):
        """Check if object with key is in the bucket.

        Returns False if object is not present, otherwise the md5string.
        """
        obj = client.Object(s3_bucket, key)
        try:
            obj.load()
            if obj.content_length != self.length:
                return False
        except ClientError as ex:
            if ex.response['Error']['Code'] == "404":
                return False
            raise
        return obj.e_tag.replace("\"", "")  # Why does AWS include this?

    def make_intermediary_key(self):
        """Return the key to use for the intermediary storage in S3."""
        padded = "{:010d}".format(self.fid)
        return "{first}/{second}/{third}/{padded}.fid".format(
            first=padded[0:1],
            second=padded[1:4],
            third=padded[4:7],
            padded=padded)

    def make_contentrepo_key(self):
        """Return the key to use for the final storage of this object in S3."""
        return self.sha1sum

    def intermediary_exists_in_bucket(self, s3_resource, s3_bucket):
        """Check if the intermediary (mogile-style key) is in the bucket."""
        return self.exists_in_bucket(s3_resource, s3_bucket,
                                     self.make_intermediary_key())

    def contentrepo_exists_in_bucket(self, s3_resource, s3_bucket):
        """Check if the final contentrepo object is in the bucket."""
        return self.exists_in_bucket(s3_resource, s3_bucket,
                                     self.make_contentrepo_key())

    def get_mogile_url(self, mogile_client):
        """Get a URL from mogile that we can use to access this file."""
        return random.choice(
            list(mogile_client.get_paths(self.dkey).data['paths'].values()))

    def copy_from_intermediary(self, s3_resource, s3_bucket):
        """Copy content from the intermediary to the final location."""
        response = s3_resource.meta.client.copy_object(
            CopySource={
                "Bucket": s3_bucket,
                "Key": self.make_intermediary_key()
            },
            Bucket=s3_bucket,
            Key=self.make_contentrepo_key())
        return response["CopyObjectResult"]["ETag"].replace("\"", "")

    def put(self, mogile_client, s3_resource, s3_bucket):
        """Put content from mogile to S3."""
        with requests.get(
                self.get_mogile_url(mogile_client), stream=True
        ) as req:
            req.raise_for_status()
            with tempfile.TemporaryFile() as tmp:
                req.raw.decode_content = True
                shutil.copyfileobj(req.raw, tmp, length=BUFSIZE)
                assert sha1_fileobj_hex(tmp) == self.sha1sum
                md5 = md5_fileobj_b64(tmp)
                tmp.seek(0)
                target = s3_resource.Object(
                    s3_bucket,
                    self.make_contentrepo_key())
                response = target.put(
                    Body=tmp,
                    ContentMD5=md5)
                return response["ETag"].replace("\"", "")

    def migrate(self, mogile_client, table, s3_resource, bucket_map):
        """Migrate this mogile object to contentrepo.

        Returns None if the object is a temporary file, otherwise
        returns the md5 of the migrated file.
        """
        if self.temp is True:
            return None  # Do not migrate temporary files.
        s3_bucket = bucket_map[self.mogile_bucket]
        print(f"Migrating {self.fid} to "
              f"s3://{s3_bucket}/{self.make_contentrepo_key()}")
        md5 = self.contentrepo_exists_in_bucket(s3_resource, s3_bucket)
        try:
            if md5 is not False:
                # Migration done!
                return True
            if self.intermediary_exists_in_bucket(s3_resource, s3_bucket):
                print(f"  Copying from s3://{s3_bucket}/"
                      f"{self.make_intermediary_key()}")
                md5 = self.copy_from_intermediary(s3_resource, s3_bucket)
                return True
            # Nothing is on S3 yet, copy content directly to the
            # final location.
            print(f"  Putting from mogile.")
            md5 = self.put(mogile_client, s3_resource, s3_bucket)
        finally:
            if md5 is not False:
                self.save_to_dynamodb(table, md5, s3_bucket)

    def save_to_dynamodb(self, table, md5, s3_bucket):
        """Save record to dynamodb certifying successful migration."""
        return table.put_item(
            Item={
                'fid': self.fid,
                'sha1': self.sha1sum,
                'md5': md5,
                'bucket': s3_bucket
            })

    def to_json(self):
        """Serialize as JSON."""
        return json.dumps({
            "length": self.length,
            "fid": self.fid,
            "dkey": self.dkey
            })

    @classmethod
    def from_json(cls, json_str):
        """Create MogileFile from JSON string."""
        return MogileFile(**json.loads(json_str))

    def verify(self, fid, md5, sha1, bucket, bucket_map, s3_resource):
        """Verify (with assert) this mogile file against asserted values."""
        print(f"Verifying {self.fid}")
        assert self.fid == fid, \
            f"fid {self.fid} not migrated"
        new_bucket = bucket_map[self.mogile_bucket]
        assert new_bucket == bucket
        s3_md5 = self.contentrepo_exists_in_bucket(s3_resource, new_bucket)
        assert (s3_md5 == md5), f"{self.fid} has wrong MD5 sum"
        assert (self.sha1sum == sha1), \
            f"{self.fid} has wrong SHA1 sum"


def get_mogile_files_from_database(database_url, limit=None, fids=None,
                                   excluded_fids=set()):
    """Return a generator for all mogile files in the database."""
    config = dj_database_url.parse(database_url)
    connection = pymysql.connect(
        host=config['HOST'],
        user=config['USER'],
        password=config['PASSWORD'],
        db=config['NAME'],
        cursorclass=pymysql.cursors.SSCursor)

    try:
        cursor = connection.cursor()
        if limit is not None:
            sql = f"SELECT * FROM file LIMIT {limit}"
        elif fids is not None:
            fids_in = ", ".join(fids)
            sql = f"SELECT * FROM file WHERE fid IN ({fids_in})"
        else:
            sql = "SELECT * FROM file"
        cursor.execute(sql)
        row = cursor.fetchone()
        while row:
            if row[0] not in excluded_fids:
                yield MogileFile.parse_row(row)
            row = cursor.fetchone()
    finally:
        connection.close()


def make_generator_from_args(args):
    """Make a mogile_file generator from args.

    Args are either a list of fids to process or a single file that
    contains a list of fids to exclude.
    """
    fids = None
    excluded_fids = set()
    if len(args) > 0:
        if args[0].isdigit():
            fids = args
        else:
            with open(args[0]) as f:
                for line in f:
                    excluded_fids.add(int(line))
            print(f"Excluding {len(excluded_fids)} fids.")
    return get_mogile_files_from_database(
        os.environ['MOGILE_DATABASE_URL'],
        fids=fids,
        excluded_fids=excluded_fids)


class QueueWorkerThread(threading.Thread):
    """Generic class for processing a work queue."""

    def __init__(self, queue: Queue, *args, **kwargs):
        """Initialize this thread."""
        super().__init__(*args, **kwargs)
        self.queue = queue
        self.caught_exception = None

    def run(self):
        """Run this thread."""
        while True:
            item = None
            try:
                item = self.queue.get_nowait()
                self.dowork(item)
            except Empty:
                break
            except BaseException as ex:
                self.caught_exception = ex
                break
            finally:
                if item is not None:
                    self.queue.task_done()

    @classmethod
    def start_pool(cls, threadCount: int, queue: Queue, *args, **kwargs):
        """Run threads on the data provided in the queue.

        Returns a list of threads to pass to `finish_pool` later.
        """
        threads = []
        for _ in range(threadCount):
            thread = cls(queue, *args, **kwargs)
            threads.append(thread)
            thread.start()
        return threads

    def join(self):
        """Join this thread, and throw exception if one was caught."""
        super().join()
        if self.caught_exception is not None:
            raise self.caught_exception

    @staticmethod
    def finish_pool(queue, threads):
        """Wait until queue is empty and threads have finished processing."""
        # block until all tasks are done
        queue.join()

        # stop workers
        for thread in threads:
            thread.join()

    @classmethod
    def process_generator(cls, threadCount, generator, *args, **kwargs):
        """Process all the items in the generator using a threadpool."""
        queue = Queue()
        counter = 0
        threads = None
        threadCount = 20
        for item in generator:
            queue.put(item)
            counter = counter + 1
            if counter == 1000:
                # Start up the consumer threads once we have 1000 entries
                threads = cls.start_pool(threadCount, queue, *args, **kwargs)
        if threads is None:
            # In case we did not get 1000 items
            threads = cls.start_pool(threadCount, queue, *args, **kwargs)
        cls.finish_pool(queue, threads)


def chunked(iterable, size):
    """Group into chunks of size."""
    result = []
    for item in iterable:
        result.append(item)
        if len(result) == size:
            yield result
            result = []
    if len(result) > 0:
        yield result