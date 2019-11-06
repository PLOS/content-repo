#!/usr/bin/env python

import base64
import hashlib
import os
import random

from botocore.exceptions import ClientError
import dj_database_url
import pymysql


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


def main():
    """Perform copy of content from mogile to S3."""
    config = dj_database_url.parse(os.environ['MOGILE_DATABASE_URL'])
    connection = pymysql.connect(
        host=config['HOST'],
        user=config['USER'],
        password=config['PASSWORD'],
        db=config['NAME'],
        cursorclass=pymysql.cursors.DictCursor)

    try:
        with connection.cursor() as cursor:
            # Read a single record
            sql = "SELECT * FROM file LIMIT 10"
            cursor.execute(sql)
            result = cursor.fetchone()
            print(result)
            print(MogileFile.parse_row(result))
    finally:
        connection.close()


def exists_in_bucket(client, bucket, path):
    """Return True if object exists in the bucket."""
    obj = client.Object(bucket, path)
    try:
        obj.load()
    except ClientError as ex:
        if ex.response['Error']['Code'] == "404":
            return False
        raise
    return True


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

    def make_mogile_path(self):
        """Return the path to use for the intermediary, mogile-path-based
storage of this file in S3."""
        padded = "{:010d}".format(self.fid)
        return "/{first}/{second}/{third}/{padded}.fid".format(
            first=padded[0:1],
            second=padded[1:4],
            third=padded[4:7],
            padded=padded)

    def make_contentrepo_path(self):
        """Return the path to use for the final storage of this file in S3."""
        return "/{}".format(self.sha1sum)

    def mogile_file_exists_in_bucket(self, client, bucket):
        """Return True if the intermediary mogile object exists in the
bucket."""
        exists_in_bucket(client, bucket, self.make_mogile_path())

    def contentrepo_file_exists_in_bucket(self, client, bucket):
        """Return True if the final contentrepo object exists in the bucket."""
        exists_in_bucket(client, bucket, self.make_contentrepo_path())

    def get_mogile_url(self, client):
        """Get a URL from mogile that we can use to access this file."""
        random.choice(list(client.get_paths(self.dkey).data['paths'].values()))


if __name__ == "__main__":
    main()
