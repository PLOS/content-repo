#!/usr/bin/env python

import os

from botocore.exceptions import ClientError
import dj_database_url
import pymysql


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

    def __init__(self, sha1sum: str, fid: int, bucket: str, length: int):
        self.sha1sum = sha1sum
        self.bucket = bucket
        self.length = length
        self.fid = fid

    @classmethod
    def parse_row(cls, row: dict):
        """Factory method to take an row as returned from the database and
return a MogileFile."""
        # Sanity check, we only use one "domain" and one class
        assert row['dmid'] == 1, "Bad domain"
        assert row['classid'] == 0, "Bad class"
        (sha1sum, orig_bucket) = row['dkey'].split('-', 1)
        return MogileFile(sha1sum=sha1sum, fid=row['fid'],
                          bucket=orig_bucket, length=row['length'])

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


if __name__ == "__main__":
    main()
