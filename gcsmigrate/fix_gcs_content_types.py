import argparse
import queue
import threading

from google.cloud import storage

from validate import (
    AmbraFile,
    BoundedThreadPoolExecutor,
    enqueue,
    logger,
    set_level,
)


def main():
    parser = argparse.ArgumentParser(description="update GCS content types from crepo")
    parser.add_argument("--host", "-H", help="mysql host")
    parser.add_argument("--port", "-P", help="mysql port")
    parser.add_argument("--user", "-u", help="mysql user")
    parser.add_argument("--password", "-p", help="mysql password")
    parser.add_argument("--limit", "-l", help="limit mysql results to L users")
    parser.add_argument("--crepo-host", "-C", help="crepo host name")
    parser.add_argument("--gcs-project", "-G", help="gcp project", default="plos-dev")
    parser.add_argument("--gcs-bucket", "-B", help="gcs bucket name")
    parser.add_argument("--log-level", "-L", default="info")

    args = parser.parse_args()
    set_level(logger, args.log_level)

    gcs = storage.Client(project=args.gcs_project)
    rowqueue = queue.Queue(maxsize=1000)

    threading.Thread(
        target=enqueue,
        args=(args.host, args.port, args.user, args.password, rowqueue),
        kwargs={"limit": args.limit},
        daemon=True,
    ).start()

    # using a standard ThreadPoolExecutor here eventually leads to OOM
    with BoundedThreadPoolExecutor(max_workers=30) as executor:
        while True:
            row = rowqueue.get()
            if row == "DONE":
                rowqueue.task_done()
                break
            executor.submit(
                process_articleFile,
                row,
                args.crepo_host,
                args.gcs_bucket,
                gcs,
                rowqueue,
            )
    rowqueue.join()


def process_articleFile(row, crepo_host, gcs_bucket, gcs_client, rowqueue):
    try:
        articlefile = AmbraFile(row, crepo_host, gcs_bucket)
        articlefile.get_crepo_data()
        if articlefile.crepo_found and articlefile.crepo_contentType:
            blob = articlefile.get_gcs_blob(gcs_client)
            if articlefile.gcs_found:
                blob.content_type = articlefile.crepo_contentType
                blob.patch()
                logger.debug(
                    f"updated content-type on {articlefile.gcs_key} to {articlefile.crepo_contentType}"
                )
    except Exception as ex:
        logger.error(f"error processing file: {ex}")
    finally:
        rowqueue.task_done()


if __name__ == "__main__":
    main()
