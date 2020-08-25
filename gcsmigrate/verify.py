import dbm.gnu
import os

from google.cloud import storage
from tqdm import tqdm

from shared import get_mogile_files_from_database, make_bucket_map


def load_bucket(bucket_name):
    gcs_client = storage.Client()
    with tqdm(desc=f"{bucket_name} list") as pbar:
        with dbm.gnu.open(bucket_name, "cf") as db:
            for page in gcs_client.list_blobs(
                bucket_name, prefix="", delimiter="/"
            ).pages:
                new_keys = {blob.name for blob in page}
                for key in new_keys:
                    # We don't need to know anything, we are just
                    # keeping track of the keys.
                    db[key] = ""
                    pbar.update()
            db.sync()
            return True


def main():
    """
    Verify transfer.

    First, list all items in the buckets and store in a gdbm file
    (because they are too large to keep in memory). Then, for each
    item in the mogile database, check in the bucket's gdbm file for
    the sha1 item.
    """
    bucket_map = make_bucket_map(os.environ["BUCKETS"])
    buckets = bucket_map.values()
    for bucket in buckets:
        load_bucket(bucket)
    dbs = {bucket: dbm.open(bucket) for bucket in buckets}
    for mogile_file in tqdm(
        get_mogile_files_from_database(os.environ["MOGILE_DATABASE_URL"])
    ):
        remote_bucket = bucket_map[mogile_file.mogile_bucket]
        assert (
            mogile_file.sha1sum in dbs[remote_bucket]
        ), f"{mogile_file.sha1sum} not in {remote_bucket}"


if __name__ == "__main__":
    main()
