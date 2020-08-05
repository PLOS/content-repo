#!/usr/bin/env python

import os
import sys

from google.cloud import pubsub_v1
from tqdm import tqdm
from shared import make_generator_from_args, future_waiter

TOPIC_ID = os.environ["TOPIC_ID"]
GCP_PROJECT = os.environ["GCP_PROJECT"]

VERIFY = b"verify"
MIGRATE = b"migrate"

# Trying to maximize throughput; we don't care about latency.
batch_settings = pubsub_v1.types.BatchSettings(
    max_messages=1000, max_bytes=10 * 1000 * 1000, max_latency=10
)

CLIENT = pubsub_v1.PublisherClient(batch_settings=batch_settings)

TOPIC_PATH = CLIENT.topic_path(GCP_PROJECT, TOPIC_ID)


def send_message(mogile_file, action):
    """Send the mogile file as an pubsub message."""
    return CLIENT.publish(TOPIC_PATH, mogile_file.to_json(), action=action)


def queue_migrate(mogile_file):
    """Queue mogile files for migration in pubsub."""
    return send_message(mogile_file, MIGRATE)


def queue_verify(mogile_file):
    """Queue mogile files for verification in pubsub."""
    return send_message(mogile_file, VERIFY)


def main():
    """Enqueue mogile file jobs to SQS for processing in AWS lambda."""
    generator = tqdm(make_generator_from_args(sys.argv[2:]))
    if sys.argv[1] == "verify":
        func = queue_verify
    else:
        func = queue_migrate
    # Call the function on all these mogile files. Each function should return a future.
    futures = (func(mogile) for mogile in generator)
    # Evaluate all the futures using our future_waiter, which will
    # stop occasionally to clean up any completed futures. This avoids
    # keeping too many results in memory.
    for f in future_waiter(futures, 10000):
        pass


if __name__ == "__main__":
    main()
