#!/usr/bin/env python

import dbm.gnu
import os
import os.path
import sys

from google.cloud import pubsub_v1, storage
from tqdm import tqdm

from shared import (
    encode_int,
    encode_json,
    future_waiter,
    get_mogile_files_from_database,
    get_state_int,
    make_bucket_map,
    make_db_connection,
    maybe_update_max,
    open_db,
)

LATEST_MOGILE_FID_KEY = "mogile_fid"
LATEST_CREPO_ID_FOR_SHA_KEY = "sha_crepo_id"
LATEST_CREPO_ID_KEY = "crepo_id"
LATEST_RHINO_FILE_ID_KEY = "rhino_file_id"

BUCKET_MAP = make_bucket_map(os.environ["BUCKETS"])
IGNORE_BUCKETS = os.environ["IGNORE_BUCKETS"].split(",")

TOPIC_ID = os.environ["TOPIC_ID"]
GCP_PROJECT = os.environ["GCP_PROJECT"]

STATE_DIR = os.environ.get("STATE_DIR", os.getcwd())
SHAS_DB_PATH = os.path.join(STATE_DIR, "shas.db")

VERIFY = b"verify"
MIGRATE = b"migrate"

# Trying to maximize throughput; we don't care about latency.
batch_settings = pubsub_v1.types.BatchSettings(
    max_messages=1000, max_bytes=10 * 1000 * 1000, max_latency=10
)

CLIENT = pubsub_v1.PublisherClient(batch_settings=batch_settings)
GCS_CLIENT = storage.Client()

TOPIC_PATH = CLIENT.topic_path(GCP_PROJECT, TOPIC_ID)


def build_shas_db(state_db):
    """Build a shas.db file where we will store the relationship between a UUID and a sha."""
    initial_id = get_state_int(state_db, LATEST_CREPO_ID_FOR_SHA_KEY)
    connection = make_db_connection(os.environ["CONTENTREPO_DATABASE_URL"])
    if not os.path.exists(SHAS_DB_PATH):
        # Need to regenerate from the beginning
        initial_id = 0
    try:
        with open_db(SHAS_DB_PATH) as db:
            cursor = connection.cursor()
            query = "SELECT id, uuid, checksum FROM objects WHERE id > %s"
            cursor.execute(query, initial_id)
            with tqdm(desc="loading shas") as pbar:
                row = cursor.fetchone()
                pbar.update()
                while row:
                    (crepo_id, uuid, checksum) = row
                    db[uuid] = checksum
                    row = cursor.fetchone()
                    pbar.update()
                    maybe_update_max(state_db, LATEST_CREPO_ID_FOR_SHA_KEY, crepo_id)
    finally:
        connection.close()


def send_message(mogile_file, action):
    """Send the mogile file as an pubsub message."""
    return CLIENT.publish(TOPIC_PATH, mogile_file.to_json(), action=action)


def queue_migrate(mogile_file, state_db):
    """Queue mogile files for migration in pubsub."""
    maybe_update_max(state_db, LATEST_MOGILE_FID_KEY, mogile_file.fid)
    return send_message(mogile_file, MIGRATE)


def queue_verify(mogile_file):
    """Queue mogile files for verification in pubsub."""
    return send_message(mogile_file, VERIFY)


def queue_rhino_final(bucket_name, state_db):
    """Queue up copies for the rhino final migration step in pubsub."""
    connection = make_db_connection(os.environ["RHINO_DATABASE_URL"])
    initial_id = get_state_int(state_db, LATEST_RHINO_FILE_ID_KEY)
    sql = """
SELECT articleFile.fileId,
       doi,
       ingestionNumber,
       ingestedFileName,
       crepoUuid
FROM articleFile
JOIN articleIngestion ON articleFile.ingestionId = articleIngestion.ingestionId
JOIN article ON articleIngestion.articleId = article.articleId
WHERE articleFile.fileId > %s
ORDER BY articleFile.fileId asc
"""
    with dbm.gnu.open(SHAS_DB_PATH) as db:
        try:
            cursor = connection.cursor()
            cursor.execute(sql, initial_id)
            row = cursor.fetchone()
            while row:
                (file_id, doi, ingestionNumber, ingestedFileName, uuid) = row
                sha = db[uuid]
                to_key = f"{doi}/{ingestionNumber}/{ingestedFileName}"
                json = {
                    "bucket": bucket_name,
                    "from_key": sha.decode("utf-8"),
                    "to_key": to_key,
                }
                yield CLIENT.publish(TOPIC_PATH, encode_json(json), action="copy")
                maybe_update_max(state_db, LATEST_RHINO_FILE_ID_KEY, file_id)
                row = cursor.fetchone()
        finally:
            connection.close()


def queue_lemur_final(buckets, state_db):
    """Queue up copies for the lemur final migration step in pubsub."""
    connection = make_db_connection(os.environ["CONTENTREPO_DATABASE_URL"])
    initial_id = get_state_int(state_db, LATEST_CREPO_ID_KEY)
    try:
        for crepo_bucket_name, gcs_bucket_name in buckets.items():
            cursor = connection.cursor()
            cursor.execute(
                "select bucketId from buckets where bucketName = %s",
                (crepo_bucket_name,),
            )
            bucket_id = cursor.fetchone()[0]
            cursor.close()
            sql = """
SELECT id,
       objects.objKey,
       checksum
  FROM (
        SELECT objKey,
               MAX(versionNumber) AS latest
          FROM objects
         WHERE bucketId = %s
           AND id > %s
         GROUP BY objKey
       ) AS m
 INNER JOIN objects
    ON objects.objKey = m.objKey
   AND objects.versionNumber = m.latest
   AND objects.bucketId = %s
"""
            cursor = connection.cursor()
            cursor.execute(sql, (bucket_id, initial_id, bucket_id))
            row = cursor.fetchone()
            while row:
                (crepo_file_id, obj_key, sha) = row
                to_key = f"{obj_key}"
                json = {
                    "bucket": gcs_bucket_name,
                    "from_key": sha,
                    "to_key": to_key,
                }
                yield CLIENT.publish(TOPIC_PATH, encode_json(json), action="copy")
                maybe_update_max(state_db, LATEST_CREPO_ID_KEY, crepo_file_id)
                row = cursor.fetchone()
            cursor.close()
    finally:
        connection.close()


def main():
    """Enqueue mogile file jobs to SQS for processing in AWS lambda.

    The first command line argument is an action, either verify or
    migrate. The following arguments are either a list of fids to
    process or a single file that contains a list of fids to exclude.

    """
    state_db = dbm.gnu.open(os.path.join(STATE_DIR, "state.db"), "cf")
    try:
        futures = None
        action = sys.argv[1]
        corpus_bucket = next(v for v in BUCKET_MAP.values() if "corpus" in v)
        non_corpus_buckets = {
            k: v
            for k, v in BUCKET_MAP.items()
            if "corpus" not in v and k not in IGNORE_BUCKETS
        }
        if action == "verify":
            generator = tqdm(
                [
                    mogile
                    for mogile in get_mogile_files_from_database(
                        os.environ["MOGILE_DATABASE_URL"]
                    )
                    if mogile.mogile_bucket not in IGNORE_BUCKETS
                ]
            )
            futures = (queue_verify(mogile) for mogile in generator)
        elif action == "migrate":
            latest_fid = get_state_int(state_db, LATEST_MOGILE_FID_KEY)
            generator = tqdm(
                [
                    mogile
                    for mogile in get_mogile_files_from_database(
                        os.environ["MOGILE_DATABASE_URL"], initial_fid=latest_fid
                    )
                    if mogile.mogile_bucket not in IGNORE_BUCKETS
                ]
            )
            futures = (queue_migrate(mogile, state_db) for mogile in generator)
        elif action == "final_migrate_rhino":
            build_shas_db(state_db)
            futures = tqdm(
                queue_rhino_final(corpus_bucket, state_db)
            )
        elif action == "final_migrate_lemur":
            build_shas_db(state_db)
            futures = tqdm(
                queue_lemur_final(non_corpus_buckets, state_db)
            )
        elif action.startswith("update_"):
            key = action[7:]
            assert key in [LATEST_CREPO_ID_KEY, LATEST_MOGILE_FID_KEY, LATEST_RHINO_FILE_ID_KEY]
            state_db[key] = encode_int(int(sys.argv[2]))
        elif action == "dump_state":
            for key in state_db.keys():
                print(f"{key} = {state_db[key]}")
        else:
            raise Exception(f"Bad action: {action}.")
        # Evaluate all the futures using our future_waiter, which will
        # stop occasionally to clean up any completed futures. This avoids
        # keeping too many results in memory.
        if futures is not None:
            for f in future_waiter(futures, 10000):
                pass
        state_db.sync()
        state_db.close()
    except:
        state_db.close()
        raise


if __name__ == "__main__":
    main()
