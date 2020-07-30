import os
import sys

from shared import make_bucket_map, get_mogile_files_from_database
import dbm.gnu
import threading
from tqdm import tqdm
from google.cloud import storage


class GCSListThread(threading.Thread):
    """
    Class to list an GCS bucket where each object starts with 0-9a-z.

    Stores results in a gdbm file.
    """

    def __init__(self, bucket_name, db, prefix, lock, pbar, *args, **kwargs):
        """Initialize this thread."""
        super().__init__(*args, **kwargs)
        self.prefix = prefix
        self.bucket_name = bucket_name
        self.db = db
        self.lock = lock
        self.gcs_client = storage.Client()
        self.caught_exception = None
        self.pbar = pbar

    def run(self):
        """Run this thread."""
        for page in self.gcs_client.list_blobs(self.bucket_name).pages:
            try:
                new_keys = {blob.name for blob in page}
                with self.lock:
                    for key in new_keys:
                        # We don't need to know anything, we are just
                        # keeping track of the keys.
                        self.db[key] = ""
                        self.pbar.update()
            except BaseException as ex:
                self.caught_exception = ex
                break

    @classmethod
    def process(cls, bucket_name):
        """Process this bucket and store results in a gdbm file."""
        with tqdm(desc=f"{bucket_name} list") as pbar:
            with dbm.gnu.open(bucket_name, 'cf') as db:
                lock = threading.Lock()
                threads = []
                for i in range(0, 16):
                    prefix = format(i, 'x')  # hex digit
                    thread = cls(bucket_name, db, prefix, lock, pbar)
                    threads.append(thread)
                    thread.start()
                for thread in threads:
                    thread.join()
                db.sync()
            return True

    def join(self):
        """Join this thread, and throw exception if one was caught."""
        super().join()
        if self.caught_exception is not None:
            raise self.caught_exception


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
        GCSListThread.process(bucket)
    dbs = {bucket: dbm.open(bucket) for bucket in buckets}
    for mogile_file in tqdm(get_mogile_files_from_database(
            os.environ['MOGILE_DATABASE_URL'])):
        remote_bucket = bucket_map[mogile_file.mogile_bucket]
        assert mogile_file.sha1sum in dbs[remote_bucket], \
            f"{mogile_file.sha1sum} not in {remote_bucket}"


if __name__ == "__main__":
    main()
