import base64
import os

import pymogilefs
from google.cloud import firestore, storage

from shared import MogileFile, make_bucket_map

COLLECTION_NAME = os.environ["COLLECTION_NAME"]
BUCKET_MAP = make_bucket_map(os.environ["BUCKETS"])

mogile_client = pymogilefs.client.Client(
    trackers=os.environ["MOGILE_TRACKERS"].split(","), domain="plos_repo"
)
firestore_client = firestore.Client()
gcs_client = storage.Client()


def main(event, context):
    """Migrate this MogileFile to GCS in GCF."""

    print(
        """This Function was triggered by messageId {} published at {}""".format(
            context.event_id, context.timestamp
        )
    )

    data = base64.b64decode(event["data"]).decode("utf-8")
    action = event["attributes"].get("action")
    mogile_file = MogileFile.from_json(data)
    collection = firestore_client.collection(COLLECTION_NAME)

    if action == "verify":
        doc_ref = collection.document(mogile_file.fid)
        doc = doc_ref.get()
        assert doc.exists(), f"No db entry for {mogile_file.fid}."
        mogile_file.verify(
            sha1=doc.get("sha1"),
            md5=doc.get("md5"),
            fid=int(doc.get("fid")),
            bucket=doc.get("bucket"),
            bucket_map=BUCKET_MAP,
            gcs_client=gcs_client,
        )
    else:
        mogile_file.migrate(mogile_client, collection, gcs_client, BUCKET_MAP)
