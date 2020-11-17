import argparse
import logging
import queue
import threading

from google.cloud import storage

from validate import AmbraFile, BoundedThreadPoolExecutor, enqueue


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)


def main():
    parser = argparse.ArgumentParser(description="verify GCS data from rhino")
    parser.add_argument("--host", "-H", help="mysql host")
    parser.add_argument("--port", "-P", help="mysql port")
    parser.add_argument("--user", "-u", help="mysql user")
    parser.add_argument("--password", "-p", help="mysql password")
    parser.add_argument("--crepo-host", "-C", help="crepo host name")
    parser.add_argument("--gcs-project", "-G", help="gcp project", default="plos-dev")
    parser.add_argument("--gcs-bucket", "-B", help="gcs bucket name")

    args = parser.parse_args()

    gcs = storage.Client(project=args.gcs_project)
    rowqueue = queue.Queue(maxsize=1000)

    threading.Thread(
        target=enqueue,
        args=(args.host, args.port, args.user, args.password, rowqueue),
        daemon=True,
    ).start()

    # using a standard ThreadPoolExecutor here eventually leads to OOM
    with BoundedThreadPoolExecutor(max_workers=30) as executor:
        while True:
            row = rowqueue.get()
            if row == "DONE":
                rowqueue.join()
                break
            executor.submit(
                process_articleFile,
                row,
                args.crepo_host,
                args.gcs_bucket,
                gcs,
            )
            rowqueue.task_done()


def process_articleFile(row, crepo_host, gcs_bucket, gcs_client):
    articlefile = AmbraFile(row, crepo_host, gcs_bucket)
    articlefile.get_crepo_data()
    if articlefile.crepo_found and articlefile.crepo_contentType:
        blob = articlefile.get_gcs_blob(gcs_client)
        if articlefile.gcs_found:
            blob.content_type = articlefile.crepo_contentType
            blob.patch()


if __name__ == "__main__":
    main()
