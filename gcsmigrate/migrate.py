import os
import sys
import time

import pymogilefs
from google.cloud import pubsub_v1, storage

from shared import (
    BATCH_SETTINGS,
    MIGRATE,
    MogileFile,
    copy_object,
    encode_json,
    make_bucket_map,
    make_db_connection,
)

BUCKET_MAP = make_bucket_map(os.environ["BUCKETS"])
TOPIC_ID = os.environ["TOPIC_ID"]
GCP_PROJECT = os.environ["GCP_PROJECT"]

mogile_client = pymogilefs.client.Client(
    trackers=os.environ["MOGILE_TRACKERS"].split(","), domain="plos_repo"
)
gcs_client = storage.Client()
pubsub_client = pubsub_v1.PublisherClient(batch_settings=BATCH_SETTINGS)
TOPIC_PATH = pubsub_client.topic_path(GCP_PROJECT, TOPIC_ID)


def main():
    """Migrate a single crepo UUID to GCS."""
    crepo_connection = make_db_connection(os.environ["CONTENTREPO_DATABASE_URL"])
    mogile_connection = make_db_connection(os.environ["MOGILE_DATABASE_URL"])
    rhino_connection = make_db_connection(os.environ["RHINO_DATABASE_URL"])
    try:
        crepo_cursor = crepo_connection.cursor()
        mogile_cursor = mogile_connection.cursor()
        rhino_cursor = rhino_connection.cursor()

        uuid = sys.stdin.readline().strip()
        while uuid:
            rhino_sql = """
SELECT doi,
       ingestionNumber,
       ingestedFileName,
       crepoKey
FROM articleFile
JOIN articleIngestion ON articleFile.ingestionId = articleIngestion.ingestionId
JOIN article ON articleIngestion.articleId = article.articleId
WHERE crepoUuid = %s;
"""
            rhino_cursor.execute(rhino_sql, (uuid,))
            (doi, ingestionNumber, ingestedFileName, crepoKey) = rhino_cursor.fetchone()

            crepo_sql = """
SELECT checksum,
       bucketName
FROM objects
JOIN buckets ON objects.bucketId = buckets.bucketId
WHERE uuid = %s
  AND objKey = %s
  AND objects.bucketId = 1
"""
            crepo_cursor.execute(crepo_sql, (uuid, crepoKey))
            (checksum, bucket) = crepo_cursor.fetchone()

            mogile_sql = "SELECT * FROM file WHERE dkey = %s and dmid = 1"
            mogile_cursor.execute(mogile_sql, (f"{checksum}-{bucket}"))
            row = mogile_cursor.fetchone()
            print(uuid)
            if row:
                mogile_file = MogileFile.parse_row(row)
                pubsub_client.publish(TOPIC_PATH, mogile_file.to_json(), action=MIGRATE)
                gcs_bucket = BUCKET_MAP[bucket]
                json = {
                    "bucket": gcs_bucket,
                    "from_key": checksum,
                    "to_key": f"{doi}/{ingestionNumber}/{ingestedFileName}",
                }
                pubsub_client.publish(TOPIC_PATH, encode_json(json), action="copy")
            uuid = sys.stdin.readline().strip()
    finally:
        crepo_cursor.close()
        mogile_cursor.close()
        rhino_cursor.close()


if __name__ == "__main__":
    main()
