"""Tools for migrating Mogile content to GCS."""

import base64
import hashlib
import io
import json
import os
import random
import shutil
import time

import dj_database_url
import pymysql
import requests

BUFSIZE = 16 * 1024


class HashWrap(io.RawIOBase):
    """Wrap a raw IO object so that when it is read, a hasher is updated."""

    def __init__(self, wrap, hasher):
        self.wrap = wrap
        self.hasher = hasher
        super().__init__()

    def tell(self):
        return self.wrap.tell()

    def readinto(self, b):
        num = self.wrap.readinto(b)
        if num is not None and num > 0:
            self.hasher.update(b[0:num])
        return num


def make_bucket_map(buckets):
    """Construct a hash from a buckets source string of the form a:b,c:d."""
    retval = {}
    for bucket in buckets.split(","):
        source, target = bucket.split(":")
        retval[source] = target
    return retval


def hash_fileobj(fileobj, hasher):
    """Efficiently hash a file object using the provided hasher."""
    fileobj.seek(0)
    block = fileobj.read(BUFSIZE)
    while block:
        hasher.update(block)
        block = fileobj.read(BUFSIZE)
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
    return base64.b64encode(hash_fileobj(fileobj, hashlib.md5()).digest()).decode(
        "utf-8"
    )


def sha1_fileobj_hex(fileobj):
    """Efficiently calculate the SHA1 sum for a file object.

    Returns a hex string.
    """
    return hash_fileobj(fileobj, hashlib.sha1()).hexdigest()


def sha1_fileobj_b64(fileobj):
    """Efficiently calculate the SHA1 sum for a file object.

    Returns a Base64 encoded string.
    """
    return base64.b64encode(hash_fileobj(fileobj, hashlib.sha1()).digest()).decode(
        "utf-8"
    )


class MogileFile:
    """Represents a file stored in mogile."""

    def __init__(self, fid: int, dkey: str, length: int):
        """Initialize a MogileFile.

        Arguments are as stored in the mogile `file` database table.
        """
        self.length = length
        self.fid = fid
        self.dkey = dkey
        if dkey == "test" or dkey[36:] == ".tmp":
            # These seem to be old junk leftover from failed ingests.
            # Check later.
            self.skip = True
            self.sha1sum = None
            self.mogile_bucket = None
        else:
            self.skip = False
            self.sha1sum = dkey[0:40]
            self.mogile_bucket = dkey[41:]
            assert len(self.sha1sum) == 40, f"bad sha1sum length for {self.dkey}"
            assert dkey[40] == "-", f"bad dkey for {self.dkey}"
            assert len(self.mogile_bucket) > 1, f"bad mogile-bucket for {self.dkey}"

    def __eq__(self, other):
        """Equality check."""
        if not isinstance(other, MogileFile):
            # Delegate comparison to the other instance's __eq__.
            return NotImplemented
        return (
            self.dkey == other.dkey
            and self.fid == other.fid
            and self.length == other.length
        )

    @classmethod
    def parse_row(cls, row):
        """Take a file row and return a MogileFile."""
        # (fid, dmid, dkey, length, classid, devcount)
        # Sanity check, we only use one "domain" and one class
        assert row[1] == 1, "Bad domain"
        assert row[4] == 0, "Bad class"
        return cls(dkey=row[2], fid=row[0], length=row[3])

    def exists_in_bucket(self, client, gcs_bucket, key):
        """Check if object with key is in the bucket.

        Returns False if object is not present, otherwise the md5string.
        """
        bucket = client.get_bucket(gcs_bucket)
        blob = bucket.get_blob(key)
        if blob is None:
            return False
        blob.reload()
        if blob.size != self.length:
            return False
        return blob.md5_hash

    def make_intermediary_key(self):
        """Return the key to use for the intermediary storage in GCS."""
        padded = "{:010d}".format(self.fid)
        return "{first}/{second}/{third}/{padded}.fid".format(
            first=padded[0:1], second=padded[1:4], third=padded[4:7], padded=padded
        )

    def make_contentrepo_key(self):
        """Return the key to use for the final storage of this object in GCS."""
        return self.sha1sum

    def intermediary_exists_in_bucket(self, gcs_client, gcs_bucket):
        """Check if the intermediary (mogile-style key) is in the bucket."""
        return self.exists_in_bucket(
            gcs_client, gcs_bucket, self.make_intermediary_key()
        )

    def contentrepo_exists_in_bucket(self, gcs_client, gcs_bucket):
        """Check if the final contentrepo object is in the bucket."""
        return self.exists_in_bucket(
            gcs_client, gcs_bucket, self.make_contentrepo_key()
        )

    def get_mogile_url(self, mogile_client):
        """Get a URL from mogile that we can use to access this file."""
        return random.choice(
            list(mogile_client.get_paths(self.dkey).data["paths"].values())
        )

    def copy_from_intermediary(self, storage_client, bucket_name):
        """Copy content from the intermediary to the final location."""
        bucket = storage_client.bucket(bucket_name)
        source_blob = bucket.blob(self.make_intermediary_key())
        blob_copy = bucket.copy_blob(source_blob, bucket, self.make_contentrepo_key())
        return blob_copy.md5_hash

    def put(self, mogile_client, gcs_client, bucket_name):
        """Put content from mogile to GCS."""
        bucket = gcs_client.get_bucket(bucket_name)
        blob = bucket.blob(self.make_contentrepo_key())
        try:
            with requests.get(self.get_mogile_url(mogile_client), stream=True) as req:
                req.raise_for_status()
                req.raw.decode_content = True
                sha1 = hashlib.sha1()
                with HashWrap(req.raw, sha1) as sha_pipe:
                    md5 = hashlib.md5()
                    with HashWrap(sha_pipe, md5) as md5_pipe:
                        blob.upload_from_file(md5_pipe, rewind=False)
                        blob.reload()
                        md5 = base64.b64encode(md5.digest()).decode("utf-8")
            assert sha1.hexdigest() == self.sha1sum
            assert md5 == blob.md5_hash
            return md5
        except:
            blob.delete()
            raise

    def migrate(self, mogile_client, collection, gcs_client, bucket_map):
        """Migrate this mogile object to contentrepo.

        Returns None if the object is a temporary file, otherwise
        returns the md5 of the migrated file.
        """
        if self.skip is True:
            return None  # Do not migrate temporary files.
        gcs_bucket = bucket_map[self.mogile_bucket]
        print(f"Migrating {self.fid} to " f"{gcs_bucket}/{self.make_contentrepo_key()}")
        md5 = self.contentrepo_exists_in_bucket(gcs_client, gcs_bucket)
        try:
            if md5 is not False:
                # Migration done!
                return True
            if self.intermediary_exists_in_bucket(gcs_client, gcs_bucket):
                print(f"  Copying from {gcs_bucket}/" f"{self.make_intermediary_key()}")
                md5 = self.copy_from_intermediary(gcs_client, gcs_bucket)
                return True
            # Nothing is on GCS yet, copy content directly to the
            # final location.
            print(f"  Putting from mogile.")
            md5 = self.put(mogile_client, gcs_client, gcs_bucket)
        finally:
            if md5 is not False:
                self.save_to_firestore(collection, md5, gcs_bucket)

    def save_to_firestore(self, collection, md5, gcs_bucket):
        """Save record to firestore certifying successful migration."""
        doc_ref = collection.document(str(self.fid))
        return doc_ref.set(
            {"fid": self.fid, "sha1": self.sha1sum, "md5": md5, "bucket": gcs_bucket}
        )

    def to_json(self):
        """Serialize as JSON."""
        return bytes(
            json.dumps({"length": self.length, "fid": self.fid, "dkey": self.dkey}),
            "utf-8",
        )

    @classmethod
    def from_json(cls, json_str):
        """Create MogileFile from JSON string."""
        return MogileFile(**json.loads(json_str))

    def verify(self, fid, md5, sha1, bucket, bucket_map, gcs_client):
        """Verify (with assert) this mogile file against asserted values."""
        print(f"Verifying {self.fid}")
        assert self.fid == fid, f"fid {self.fid} not migrated"
        new_bucket = bucket_map[self.mogile_bucket]
        assert new_bucket == bucket
        remote_md5 = self.contentrepo_exists_in_bucket(gcs_client, new_bucket)
        assert remote_md5 == md5, f"{self.fid} has wrong MD5 sum"
        assert self.sha1sum == sha1, f"{self.fid} has wrong SHA1 sum"


def get_mogile_files_from_database(
    database_url, limit=None, fids=None, excluded_fids=set()
):
    """Return a generator for all mogile files in the database."""
    config = dj_database_url.parse(database_url)
    connection = pymysql.connect(
        host=config["HOST"],
        user=config["USER"],
        password=config["PASSWORD"],
        db=config["NAME"],
        cursorclass=pymysql.cursors.SSCursor,
    )

    try:
        cursor = connection.cursor()
        if limit is not None:
            sql = f"SELECT * FROM file LIMIT {limit}"
        elif fids is not None:
            fids_in = ", ".join(fids)
            sql = f"SELECT * FROM file WHERE fid IN ({fids_in})"
        else:
            sql = "SELECT * FROM file"
        cursor.execute(sql)
        row = cursor.fetchone()
        while row:
            if row[0] not in excluded_fids:
                yield MogileFile.parse_row(row)
            row = cursor.fetchone()
    finally:
        connection.close()


def make_generator_from_args(args):
    """Make a mogile_file generator from args.

    Args are either a list of fids to process or a single file that
    contains a list of fids to exclude.
    """
    fids = None
    excluded_fids = set()
    if len(args) > 0:
        if args[0].isdigit():
            fids = args
        else:
            with open(args[0]) as f:
                for line in f:
                    excluded_fids.add(int(line))
            print(f"Excluding {len(excluded_fids)} fids.")
    return get_mogile_files_from_database(
        os.environ["MOGILE_DATABASE_URL"], fids=fids, excluded_fids=excluded_fids
    )


def future_waiter(iterable, max_futures):
    def cleanup(lst):
        """Yield the result of any futures from the lst that are done, and delete them from the list."""
        to_delete = []
        for i, future in enumerate(lst):
            if future.done():
                to_delete.append(i)
                yield future.result()

        # we need to delete back-to-front to avoid messing up the index
        to_delete.reverse()
        for i in to_delete:
            del lst[i]

    not_done = []

    for future in iterable:
        not_done.append(future)
        if len(not_done) > max_futures:
            yield from cleanup(not_done)
    while (len(not_done)) > 0:
        time.sleep(1)
        yield from cleanup(not_done)
