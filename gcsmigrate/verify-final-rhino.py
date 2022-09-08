"""Verify the "final migrate" stage for rhino.

First this will list all items in the corpus bucket under 10.1371, storing their path and size in a GNU db file.

Then, we will get all files from rhino and verify that they have been correctly transfered to GCS.
"""

import dbm.gnu
import os

from google.cloud import storage
from tqdm import tqdm
import requests
from shared import load_bucket, make_bucket_map, make_db_connection, open_db
import base64
import crc32c

BUCKET_MAP = make_bucket_map(os.environ["BUCKETS"])
GCS_BUCKET = "plos-corpus-prod"
MOGILE_BUCKET = "mogilefs-prod-repo"

client = storage.Client()
bucket = client.bucket(GCS_BUCKET)

# Load all items in the bucket.
load_bucket(GCS_BUCKET, "corpus.db", prefix="10.1371", delimiter="")

connection = make_db_connection(os.environ["RHINO_DATABASE_URL"])
SQL = """
SELECT concat(article.doi, "/", articleIngestion.ingestionNumber, "/", articleFile.ingestedFileName),
       articleFile.crepoKey,
       articleFile.crepoUuid
FROM articleFile
JOIN articleIngestion ON articleFile.ingestionId = articleIngestion.ingestionId
JOIN article ON article.articleId = articleIngestion.articleId;
"""

def calc_crepo_crc(
db = dbm.gnu.open("corpus.db")
for key in db.keys():
    print(key)
    gcs_crc = int.from_bytes(base64.b64decode(db[key]), byteorder="big")
    versions_base_url = "http://journals-prod1-contentrepo1.soma.plos.org:8002/v1/objects/versions/mogilefs-prod-repo"
    versions = requests.get(versions_base_url, params={"key": key})
    url = "http://journals-prod1-contentrepo1.soma.plos.org:8002/v1/objects/mogilefs-prod-repo?key=10.1371%2fjournal.pbio.0030204.q001.PNG&version=0"

    crepo_crc = 0
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        for chunk in r.iter_content(chunk_size=8192):
            crepo_crc = crc32c.crc32c(chunk, crepo_crc)
    assert crepo_crc == gcs_crc, f"bad crc for {url}"

cursor = connection.cursor()
with tqdm() as pbar:
    cursor.execute(SQL)
    row = cursor.fetchone()
    while row:
        pbar.update()
        (key, uuid) = row
        if key in db:
            crc = int.from_bytes(db[key], byteorder="little")
            if size != gcs_size:
                print(uuid)
        else:
            print(uuid)
        row = cursor.fetchone()
