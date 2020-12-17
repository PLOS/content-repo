import json
import base64
import os

import pymogilefs
from google.cloud import firestore, storage

from shared import MogileFile, make_bucket_map, copy_object

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

    json_str = base64.b64decode(event["data"]).decode("utf-8")
    json_struct = json.loads(json_str)
    action = event["attributes"].get("action")
    collection = firestore_client.collection(COLLECTION_NAME)

    if action == "verify":
        mogile_file = MogileFile.from_json(json_struct)
        doc_ref = collection.document(str(mogile_file.fid))
        doc = doc_ref.get()
        assert doc.exists, f"No db entry for {mogile_file.fid}."
        mogile_file.verify(
            sha1=doc.get("sha1"),
            md5=doc.get("md5"),
            fid=int(doc.get("fid")),
            bucket=doc.get("bucket"),
            bucket_map=BUCKET_MAP,
            gcs_client=gcs_client,
        )
    elif action == "migrate":
        mogile_file = MogileFile.from_json(json_struct)
        mogile_file.migrate(mogile_client, collection, gcs_client, BUCKET_MAP)
    elif action == "copy":
        bucket_name = json_struct["bucket"]
        from_key = json_struct["from_key"]
        to_key = json_struct["to_key"]
        copy_object(gcs_client, bucket_name, from_key, to_key)
    else:
        raise Exception(f"Bad action: {action}.")
