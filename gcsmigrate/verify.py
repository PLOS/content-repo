"""Verify transfer.

First, list all items in the buckets and store in a gdbm file (because
they are too large to keep in memory). Then, for each item in the
mogile database, check in the bucket's gdbm file for the sha1 item.

"""
import dbm.gnu
import os
import sys

from tqdm import tqdm

from shared import get_mogile_files_from_database, make_bucket_map, load_bucket


def main():
    bucket_map = make_bucket_map(os.environ["BUCKETS"])
    ignore_buckets = os.environ["IGNORE_BUCKETS"].split(",")
    buckets = bucket_map.values()
    for bucket in buckets:
        load_bucket(bucket, f"{bucket}.db")
    dbs = {bucket: dbm.open(f"{bucket}.db") for bucket in buckets}
    failed = False
    for mogile_file in tqdm(
        get_mogile_files_from_database(os.environ["MOGILE_DATABASE_URL"])
    ):
        if mogile_file.skip:
            continue
        if mogile_file.mogile_bucket in ignore_buckets:
            continue
        remote_bucket = bucket_map[mogile_file.mogile_bucket]
        if mogile_file.sha1sum not in dbs[remote_bucket]:
            print(mogile_file.fid)
            failed = True
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
