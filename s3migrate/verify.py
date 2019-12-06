import os
import sys

from shared import make_bucket_map, get_mogile_files_from_database
import boto3
import dbm.gnu
import threading


class S3ListThread(threading.Thread):
    """
    Class to list an S3 bucket where each object starts with 0-9a-z.

    Stores results in a gdbm file.
    """

    def __init__(self, bucket_name, db, prefix, lock, *args, **kwargs):
        """Initialize this thread."""
        super().__init__(*args, **kwargs)
        self.prefix = prefix
        self.bucket_name = bucket_name
        self.db = db
        self.lock = lock
        self.s3 = boto3.client('s3')
        self.caught_exception = None

    def run(self):
        """Run this thread."""
        kwargs = {'Bucket': self.bucket_name, "Prefix": self.prefix}
        while True:
            try:
                sys.stderr.write("*")
                sys.stderr.flush()
                resp = self.s3.list_objects_v2(**kwargs)
                if "Contents" not in resp:
                    break
                new_keys = {obj["Key"] for obj in resp["Contents"]}
                with self.lock:
                    for key in new_keys:
                        # We don't need to know anything, we are just
                        # keeping track of the keys.
                        self.db[key] = ""
                if resp["IsTruncated"]:
                    kwargs['ContinuationToken'] = resp['NextContinuationToken']
                else:
                    break
            except BaseException as ex:
                self.caught_exception = ex
                break

    @classmethod
    def process(cls, bucket_name):
        """Process this bucket and store results in a gdbm file."""
        with dbm.gnu.open(bucket_name, 'cf') as db:
            lock = threading.Lock()
            threads = []
            for i in range(0, 16):
                prefix = format(i, 'x')  # hex digit
                thread = cls(bucket_name, db, prefix, lock)
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
        S3ListThread.process(bucket)
    dbs = {bucket: dbm.open(bucket) for bucket in buckets}
    for mogile_file in get_mogile_files_from_database(
            os.environ['MOGILE_DATABASE_URL']):
        s3_bucket = bucket_map[mogile_file.mogile_bucket]
        assert mogile_file.sha1sum in dbs[s3_bucket], \
            f"{mogile_file.sha1sum} not in {s3_bucket}"


if __name__ == "__main__":
    main()
