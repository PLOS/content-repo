import os

from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
from google.cloud import firestore
import pymogilefs
from google.cloud import storage

bucket_map = make_bucket_map(os.environ["BUCKETS"])
mogile_client = pymogilefs.client.Client(
    trackers=os.environ["MOGILE_TRACKERS"].split(","), domain="plos_repo"
)

PROJECT_ID = os.environ["PROJECT_ID"]
SUBSCRIPTION_ID = os.environ["SUBSCRIPTION_ID"]
COLLECTION_NAME = os.environ["COLLECTION_NAME"]
TIMEOUT = 5.0

subscriber = pubsub_v1.SubscriberClient()
SUBSCRIPTION_PATH = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)

firestore_client = firestore.Client()
gcs_client = storage.Client()


# https://console.cloud.google.com/storage/browser/[bucket-id]/
from shared import MogileFile, make_bucket_map

def callback(message):
    """Migrate this MogileFile to GCS in GCF."""
    action = message.attributes.get("action")
    mogile_file = MogileFile.from_json(message.data)
    collection = firestore_client.collection(COLLECTION_NAME)

    if action == "verify":
        doc_ref = collection.document(mogile_file.fid)
        doc = doc_ref.get()
        assert doc.exists() , f"No db entry for {mogile_file.fid}."
        mogile_file.verify(
            sha1=doc.get('sha1'),
            md5=doc.get('md5'),
            fid=int(doc.get('fid')),
            bucket=doc.get('bucket'),
            bucket_map=bucket_map,
            gcs_client=gcs_client)
    else:
        mogile_file.migrate(mogile_client, collection, gcs_client, bucket_map)


streaming_pull_future = subscriber.subscribe(SUBSCRIPTION_PATH, callback=callback)
print("Listening for messages on {}..\n".format(SUBSCRIPTION_PATH))

# Wrap subscriber in a 'with' block to automatically call close() when done.
with subscriber:
    try:
        # When `timeout` is not set, result() will block indefinitely,
        # unless an exception is encountered first.
        streaming_pull_future.result(timeout=TIMEOUT)
    except TimeoutError:
        streaming_pull_future.cancel()

