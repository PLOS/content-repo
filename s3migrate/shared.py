"""Tools for migrating Mogile content to S3."""

import base64
import hashlib
import json
import random
import shutil
import tempfile

import dj_database_url
import pymysql
import requests
from botocore.exceptions import ClientError


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
        if dkey.endswith('.tmp'):
            # These seem to be old junk leftover from failed ingests.
            # Check later.
            self.temp = True
            self.sha1sum = None
            self.mogile_bucket = None
        else:
            self.temp = False
            (self.sha1sum, self.mogile_bucket) = dkey.split('-', 1)

    def __eq__(self, other):
        if not isinstance(other, MogileFile):
            # Delegate comparison to the other instance's __eq__.
            return NotImplemented
        return(self.dkey == other.dkey and
               self.fid == other.fid and
               self.length == other.length)

    @classmethod
    def parse_row(cls, row: dict):
        """Take a file row and return a MogileFile."""
        # Sanity check, we only use one "domain" and one class
        assert row['dmid'] == 1, "Bad domain"
        assert row['classid'] == 0, "Bad class"
        return cls(dkey=row['dkey'],
                   fid=row['fid'],
                   length=row['length'])

    def exists_in_bucket(self, client, s3_bucket, key):
        """Check if object with key is in the bucket."""
        obj = client.Object(s3_bucket, key)
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

    def intermediary_exists_in_bucket(self, s3_client, s3_bucket):
        """Check if the intermediary (mogile-style key) is in the bucket."""
        return self.exists_in_bucket(s3_client, s3_bucket,
                                     self.make_intermediary_key())

    def contentrepo_exists_in_bucket(self, s3_client, s3_bucket):
        """Check if the final contentrepo object is in the bucket."""
        return self.exists_in_bucket(s3_client, s3_bucket,
                                     self.make_contentrepo_key())

    def get_mogile_url(self, mogile_client):
        """Get a URL from mogile that we can use to access this file."""
        return random.choice(
            list(mogile_client.get_paths(self.dkey).data['paths'].values()))

    def copy_from_intermediary(self, s3_client, s3_bucket):
        """Copy content from the intermediary to the final location."""
        obj = s3_client.Object(
            s3_bucket,
            self.make_contentrepo_key())
        obj.copy({
            'Bucket': s3_bucket,
            'Key': self.make_intermediary_key()
        })

    def put(self, mogile_client, s3_client, s3_bucket):
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
                    s3_bucket,
                    self.make_contentrepo_key())
                target.put(
                    Body=tmp,
                    ContentMD5=md5)

    def migrate(self, mogile_client, s3_client, bucket_map):
        """Migrate this mogile object to contentrepo."""
        if self.temp is True:
            pass  # Do not migrate temporary files.
        else:
            s3_bucket = bucket_map[self.mogile_bucket]
            print(f"Migrating {self.fid} to "
                  f"s3://{s3_bucket}/{self.make_contentrepo_key()}")
            if self.contentrepo_exists_in_bucket(s3_client, s3_bucket):
                pass  # Migration done!
            else:
                if self.intermediary_exists_in_bucket(s3_client, s3_bucket):
                    self.copy_from_intermediary(s3_client, s3_bucket)
                else:
                    # Nothing is on S3 yet, copy content directly to the
                    # final location.
                    self.put(mogile_client, s3_client, s3_bucket)

    def to_json(self):
        return json.dumps({
            "length": self.length,
            "fid": self.fid,
            "dkey": self.dkey
            })

    @classmethod
    def from_json(cls, json_str):
        return MogileFile(**json.loads(json_str))

def get_mogile_files_from_database(database_url):
    config = dj_database_url.parse(database_url)
    connection = pymysql.connect(
        host=config['HOST'],
        user=config['USER'],
        password=config['PASSWORD'],
        db=config['NAME'],
        cursorclass=pymysql.cursors.SSDictCursor)

    try:
        with connection.cursor() as cursor:
            row_count = 1000
            sql = "SELECT * FROM file"
            cursor.execute(sql)
            rows = cursor.fetchmany(row_count)
            while len(rows) != 0:
                for row in rows:
                    yield MogileFile.parse_row(row)
                rows = cursor.fetchmany(row_count)
    finally:
        connection.close()
