"""Verify the "final migrate" stage for rhino.

First this will list all items in the corpus bucket under 10.1371, storing their path and size in a GNU db file.

Then, we will get all files from rhino and verify that they have been correctly transfered to GCS.
"""

import dbm.gnu
import os

from google.cloud import storage
from tqdm import tqdm

from shared import load_bucket, make_bucket_map, make_db_connection, open_db

BUCKET_MAP = make_bucket_map(os.environ["BUCKETS"])
CORPUS_BUCKET = next(v for v in BUCKET_MAP.values() if "corpus" in v)

client = storage.Client()
bucket = client.bucket(CORPUS_BUCKET)

# Load all items in the bucket.
load_bucket(CORPUS_BUCKET, "corpus.db", prefix="10.1371", delimiter="")

connection = make_db_connection(os.environ["RHINO_DATABASE_URL"])
SQL = """
SELECT concat(article.doi, "/", articleIngestion.ingestionNumber, "/", articleFile.ingestedFileName),
       articleFile.fileSize,
       articleFile.crepoUuid
FROM articleFile
JOIN articleIngestion ON articleFile.ingestionId = articleIngestion.ingestionId
JOIN article ON article.articleId = articleIngestion.articleId;
"""
db = dbm.gnu.open("corpus.db")
cursor = connection.cursor()
with tqdm() as pbar:
    cursor.execute(SQL)
    row = cursor.fetchone()
    while row:
        pbar.update()
        (key, size, uuid) = row
        if key in db:
            gcs_size = int.from_bytes(db[key], byteorder="little")
            if size != gcs_size:
                print(uuid)
        else:
            print(uuid)
        row = cursor.fetchone()
