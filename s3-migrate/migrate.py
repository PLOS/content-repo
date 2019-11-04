#!/usr/bin/env python

import os

import dj_database_url
import pymysql


def main():
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
            print(MogileFile.parse_entry(result))
    finally:
        connection.close()


class MogileFile():
    def __init__(self, sha1sum: str, fid: int, bucket: str, length: int):
        self.sha1sum = sha1sum
        self.bucket = bucket
        self.length = length
        self.fid = fid

    @classmethod
    def parse_entry(cls, entry: dict):
        # Sanity check, we only use one "domain" and one class
        assert entry['dmid'] == 1, "Bad domain"
        assert entry['classid'] == 0, "Bad class"
        (sha1sum, orig_bucket) = entry['dkey'].split('-', 1)
        return MogileFile(sha1sum=sha1sum, fid=entry['fid'],
                          bucket=orig_bucket, length=entry['length'])

    def make_mogile_path(self):
        padded = "{:010d}".format(self.fid)
        return "/{first}/{second}/{third}/{padded}.fid".format(
            first=padded[0:1],
            second=padded[1:4],
            third=padded[4:7],
            padded=padded)

    def make_contentrepo_path(self):
        return "/{}".format(self.sha1sum)

if __name__ == "__main__":
    main()
