import dbm.gnu
import os
from contextlib import contextmanager

from google.cloud import storage
from tqdm import tqdm

from shared import get_mogile_files_from_database, make_bucket_map


@contextmanager
def maybe_db(dbpath):
    if os.path.exists(dbpath):
        yield None
    else:
        with dbm.gnu.open(dbpath, "cf") as db:
            try:
                yield db
                db.sync()
            except:
                # Clean up db if there was an error
                os.unlink(dbpath)
                raise

def load_bucket(bucket_name):
    gcs_client = storage.Client()
    with maybe_db(f"{bucket_name}.db") as db:
        if db is None:
            return
        with tqdm(desc=f"{bucket_name} list") as pbar:
            for page in gcs_client.list_blobs(
                bucket_name, prefix="", delimiter="/"
            ).pages:
                new_keys = {blob.name for blob in page}
                for key in new_keys:
                    # We don't need to know anything, we are just
                    # keeping track of the keys.
                    db[key] = ""
                    pbar.update()


def load_mogile():
    with maybe_db("mogile.db") as db:
        if db is None:
            return
        for mogile_file in tqdm(
            get_mogile_files_from_database(os.environ["MOGILE_DATABASE_URL"])
        ):
            if mogile_file.skip:
                continue
            db[f"{mogile_file.fid}_{mogile_file.sha1sum}_{mogile_file.mogile_bucket}"] = ""


def main():
    """
    Verify transfer.

    First, list all items in the buckets and store in a gdbm file
    (because they are too large to keep in memory). Then, for each
    item in the mogile database, check in the bucket's gdbm file for
    the sha1 item.
    """
    bucket_map = make_bucket_map(os.environ["BUCKETS"])
    ignore_buckets = os.environ["IGNORE_BUCKETS"].split(",")
    buckets = bucket_map.values()
    for bucket in buckets:
        load_bucket(bucket)
    dbs = {bucket: dbm.open(f"{bucket}.db") for bucket in buckets}
    load_mogile()
    mogile_db = dbm.open("mogile.db")
    with tqdm(desc="verifying") as pbar:
        key = mogile_db.firstkey()
        while key is not None:
            fid, sha1sum, mogile_bucket = key.decode("utf-8").split("_")
            key = mogile_db.nextkey(key)
            pbar.update()
            if mogile_bucket in ignore_buckets:
                continue
            remote_bucket = bucket_map[mogile_bucket]
            assert sha1sum in dbs[remote_bucket], f"{sha1sum} not in {remote_bucket}"

if __name__ == "__main__":
    main()
