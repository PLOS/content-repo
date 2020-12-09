"""Verify transfer.

First, list all items in the buckets and store in a gdbm file (because
they are too large to keep in memory). Then, for each item in the
mogile database, check in the bucket's gdbm file for the sha1 item.

"""
import dbm.gnu
import os
import sys

from google.cloud import storage
from tqdm import tqdm

from shared import get_mogile_files_from_database, make_bucket_map, open_db, load_bucket


def load_mogile():
    dbpath = "mogile.db"
    if os.path.exists(dbpath):
        return
    with open_db("mogile.db") as db:
        for mogile_file in tqdm(
            get_mogile_files_from_database(os.environ["MOGILE_DATABASE_URL"])
        ):
            if mogile_file.skip:
                continue
            db[
                f"{mogile_file.fid}_{mogile_file.sha1sum}_{mogile_file.mogile_bucket}"
            ] = ""


def main():
    bucket_map = make_bucket_map(os.environ["BUCKETS"])
    ignore_buckets = os.environ["IGNORE_BUCKETS"].split(",")
    buckets = bucket_map.values()
    for bucket in buckets:
        load_bucket(bucket, f"{bucket}.db")
    dbs = {bucket: dbm.open(f"{bucket}.db") for bucket in buckets}
    load_mogile()
    mogile_db = dbm.open("mogile.db")
    failed = False
    with tqdm(desc="verifying") as pbar:
        key = mogile_db.firstkey()
        while key is not None:
            fid, sha1sum, mogile_bucket = key.decode("utf-8").split("_")
            key = mogile_db.nextkey(key)
            pbar.update()
            if mogile_bucket in ignore_buckets:
                continue
            remote_bucket = bucket_map[mogile_bucket]
            if sha1sum not in dbs[remote_bucket]:
                print(fid)
                failed = True
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
