#!/usr/bin/env python

import dbm.gnu
import os
import sys

from google.cloud import pubsub_v1, storage
from tqdm import tqdm

from shared import (
    future_waiter,
    make_db_connection,
    make_generator_from_args,
    open_db,
    encode_json,
)

TOPIC_ID = os.environ["TOPIC_ID"]
GCP_PROJECT = os.environ["GCP_PROJECT"]

VERIFY = b"verify"
MIGRATE = b"migrate"

# Trying to maximize throughput; we don't care about latency.
batch_settings = pubsub_v1.types.BatchSettings(
    max_messages=1000, max_bytes=10 * 1000 * 1000, max_latency=10
)

CLIENT = pubsub_v1.PublisherClient(batch_settings=batch_settings)
GCS_CLIENT = storage.Client()

TOPIC_PATH = CLIENT.topic_path(GCP_PROJECT, TOPIC_ID)


def build_shas_db():
    """Build a shas.db file where we will store the relationship between a UUID and a sha."""
    connection = make_db_connection(os.environ["CONTENTREPO_DATABASE_URL"])
    try:
        with open_db("shas.db") as db:
            cursor = connection.cursor()
            cursor.execute("select uuid, checksum from objects")
            with tqdm(desc="loading shas") as pbar:
                row = cursor.fetchone()
                pbar.update()
                while row:
                    (uuid, checksum) = row
                    db[uuid] = checksum
                    row = cursor.fetchone()
                    pbar.update()
    finally:
        connection.close()


def send_message(mogile_file, action):
    """Send the mogile file as an pubsub message."""
    return CLIENT.publish(TOPIC_PATH, mogile_file.to_json(), action=action)


def queue_migrate(mogile_file):
    """Queue mogile files for migration in pubsub."""
    return send_message(mogile_file, MIGRATE)


def queue_verify(mogile_file):
    """Queue mogile files for verification in pubsub."""
    return send_message(mogile_file, VERIFY)


def queue_final_migrate():
    """Queue up copies for the final migration step in pubsub."""
    bucket_name = "corpus-dev-0242ac130003" # TODO un-hardcode
    connection = make_db_connection(os.environ["RHINO_DATABASE_URL"])
    sql = "SELECT doi, ingestionNumber, ingestedFileName, crepoUuid FROM articleFile JOIN articleIngestion ON articleFile.ingestionId = articleIngestion.ingestionId JOIN article ON articleIngestion.articleId = article.articleId;"
    bucket = GCS_CLIENT.bucket(bucket_name)
    with dbm.gnu.open("shas.db") as db:
        try:
            cursor = connection.cursor()
            cursor.execute(sql)
            row = cursor.fetchone()
            while row:
                (doi, ingestionNumber, ingestedFileName, uuid) = row
                sha = db[uuid]
                to_key = f"{doi}/{ingestionNumber}/{ingestedFileName}"
                if not bucket.blob(to_key).exists():
                    json = {
                        "bucket": bucket_name,
                        "from_key": sha.decode("utf-8"),
                        "to_key": to_key,
                    }
                    yield CLIENT.publish(TOPIC_PATH, encode_json(json), action="copy")
                row = cursor.fetchone()
        finally:
            connection.close()


def main():
    """Enqueue mogile file jobs to SQS for processing in AWS lambda.

    The first command line argument is an action, either verify or
    migrate. The following arguments are either a list of fids to
    process or a single file that contains a list of fids to exclude.

    """

    action = sys.argv[1]
    if action == "verify":
        generator = tqdm(make_generator_from_args(sys.argv[2:]))
        futures = (queue_verify(mogile) for mogile in generator)
    elif action == "migrate":
        generator = tqdm(make_generator_from_args(sys.argv[2:]))
        futures = (queue_migrate(mogile) for mogile in generator)
    elif action == "final_migrate":
        build_shas_db()
        futures = tqdm(queue_final_migrate())
    else:
        raise Exception(f"Bad action: {action}.")
    # Evaluate all the futures using our future_waiter, which will
    # stop occasionally to clean up any completed futures. This avoids
    # keeping too many results in memory.
    for f in future_waiter(futures, 10000):
        pass


if __name__ == "__main__":
    main()
