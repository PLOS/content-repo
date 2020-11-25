import os
import sys
import time

import pymogilefs
from google.cloud import firestore, storage

from shared import MogileFile, make_bucket_map, make_db_connection, copy_object, guess_mimetype

COLLECTION_NAME = os.environ["COLLECTION_NAME"]
BUCKET_MAP = make_bucket_map(os.environ["BUCKETS"])

mogile_client = pymogilefs.client.Client(
    trackers=os.environ["MOGILE_TRACKERS"].split(","), domain="plos_repo"
)
firestore_client = firestore.Client()
gcs_client = storage.Client()
collection = firestore_client.collection(COLLECTION_NAME)


def main():
    """Migrate a single crepo UUID to GCS."""
    uuid = sys.argv[1]
    crepo_connection = make_db_connection(os.environ["CONTENTREPO_DATABASE_URL"])
    mogile_connection = make_db_connection(os.environ["MOGILE_DATABASE_URL"])
    rhino_connection = make_db_connection(os.environ["RHINO_DATABASE_URL"])
    try:
        crepo_cursor = crepo_connection.cursor()
        mogile_cursor = mogile_connection.cursor()
        rhino_cursor = rhino_connection.cursor()

        rhino_sql = """
SELECT doi,
       ingestionNumber,
       ingestedFileName
FROM articleFile
JOIN articleIngestion ON articleFile.ingestionId = articleIngestion.ingestionId
JOIN article ON articleIngestion.articleId = article.articleId
WHERE crepoUuid = %s;
"""
        rhino_cursor.execute(rhino_sql, (uuid,))
        (doi, ingestionNumber, ingestedFileName) = rhino_cursor.fetchone()

        crepo_sql = """
SELECT checksum, bucketName
FROM objects
JOIN buckets ON objects.bucketId = buckets.bucketId
WHERE uuid = %s;
"""
        crepo_cursor.execute(crepo_sql, (uuid,))
        (checksum, bucket) = crepo_cursor.fetchone()

        mogile_sql = "SELECT * FROM file WHERE dkey = %s"
        mogile_cursor.execute(mogile_sql, (f"{checksum}-{bucket}"))
        mogile_file = MogileFile.parse_row(mogile_cursor.fetchone())

        mogile_file.migrate(mogile_client, collection, gcs_client, BUCKET_MAP)
        gcs_bucket = BUCKET_MAP[bucket]
        copy_object(
            gcs_client,
            gcs_bucket,
            checksum,
            f"{doi}/{ingestionNumber}/{ingestedFileName}",
            guess_mimetype(ingestedFileName)
        )
    finally:
        crepo_cursor.close()
        mogile_cursor.close()
        rhino_cursor.close()


if __name__ == "__main__":
    main()
