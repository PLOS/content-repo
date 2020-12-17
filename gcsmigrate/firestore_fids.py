"""Dump all fids from Firestore."""
import os

from google.cloud import firestore
from tqdm import tqdm

COLLECTION_NAME = os.environ["COLLECTION_NAME"]

AT_ONCE = 10000


def main():
    """Print all processed fids to stdout."""
    firestore_client = firestore.Client()
    collection = firestore_client.collection(COLLECTION_NAME)
    cursor = None
    done = False
    with tqdm() as pbar:
        while not done:
            if cursor is not None:
                stream = (
                    collection.limit(AT_ONCE).order_by("fid").start_after(cursor).stream()
                )
            else:
                stream = collection.limit(AT_ONCE).order_by("fid").stream()

            for n, doc in enumerate(stream):
                print(doc.get("fid"))
                pbar.update(1)
                if n == (AT_ONCE - 1):
                    cursor = doc.reference.get()

if __name__ == "__main__":
    main()
