import os

from google.cloud import firestore

COLLECTION_NAME = os.environ["COLLECTION_NAME"]

def main():
    """Print all processed fids to stdout."""
    firestore_client = firestore.Client()
    collection = firestore_client.collection(COLLECTION_NAME)
    docs = collection.select(['fid']).stream()
    for doc in docs:
        print(doc.fid)


if __name__ == "__main__":
    main()
