import argparse
from datetime import datetime
import queue
import subprocess
import threading

from google.cloud import storage

from validate import AmbraFile, BoundedThreadPoolExecutor, enqueue, logger, set_level


def main():
    parser = argparse.ArgumentParser(description="update GCS content types from crepo")
    parser.add_argument("--host", "-H", help="mysql host")
    parser.add_argument("--port", "-P", help="mysql port")
    parser.add_argument("--user", "-u", help="mysql user")
    parser.add_argument("--password", "-p", help="mysql password")
    parser.add_argument("--crepo-host", "-C", help="crepo host name")
    parser.add_argument("--gcs-project", "-G", help="gcp project", default="plos-dev")
    parser.add_argument("--gcs-bucket", "-B", help="gcs source bucket name")
    parser.add_argument("--log-level", "-L", default="info")

    args = parser.parse_args()
    set_level(logger, args.log_level)

    gcs = storage.Client(project=args.gcs_project)
    test_bucket = create_test_bucket(gcs)
    source_bucket = gcs.bucket(args.gcs_bucket)

    populate_test_bucket(
        args.host, args.port, args.user, args.password, source_bucket, test_bucket
    )
    run_fix(
        args.host,
        args.port,
        args.user,
        args.password,
        args.crepo_host,
        args.gcs_project,
        test_bucket.name,
    )
    logger.info("Validating results ... ")
    validate_test_bucket(
        args.host, args.port, args.user, args.password, args.crepo_host, test_bucket
    )
    logger.info(f"Deleting test bucket {test_bucket.name}")
    test_bucket.delete(force=True)


def create_test_bucket(gcs):
    test_name = f"test_gcs_contenttype_fix_{datetime.now().strftime('%s')}"
    bucket = gcs.bucket(test_name)
    try:
        bucket = gcs.create_bucket(bucket, location="us-east1")
        logger.info(f"created test bucket {test_name}")
        return bucket
    except Exception as ex:
        logger.error(f"failed to create test bucket {test_name}")


def populate_test_bucket(host, port, user, password, source_bucket, test_bucket):
    rowqueue = queue.Queue(maxsize=1000)

    threading.Thread(
        target=enqueue,
        args=(host, port, user, password, rowqueue),
        kwargs={"limit": 100},
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
                populate_bucket_consumer,
                row,
                source_bucket,
                test_bucket,
                rowqueue,
            )
    rowqueue.join()


def populate_bucket_consumer(row, source_bucket, test_bucket, rowqueue):
    try:
        gcskey = f"{row['doi']}/{row['ingestionNumber']}/{row['ingestedFileName']}"
        source_blob = source_bucket.blob(gcskey)
        if source_blob.exists():
            try:
                source_bucket.copy_blob(source_blob, test_bucket, gcskey)
                logger.info(f"copied {gcskey} to {test_bucket.name}")
            except Exception as ex:
                logger.error(f"error copying {gcskey} to {test_bucket.name}: {ex}")
    finally:
        rowqueue.task_done()


def run_fix(host, port, user, password, crepo_host, project, bucket):
    logger.info(f"Running update script on test bucket {bucket} ... ")
    subprocess.check_call(
        [
            "pipenv",
            "run",
            "python",
            "fix_gcs_content_types.py",
            "-H",
            host,
            "-P",
            port,
            "-u",
            user,
            "-p",
            password,
            "-l",
            "100",
            "-C",
            crepo_host,
            "-G",
            project,
            "-B",
            bucket,
        ]
    )


def validate_test_bucket(host, port, user, password, crepo_host, test_bucket):
    rowqueue = queue.Queue(maxsize=1000)

    threading.Thread(
        target=enqueue,
        args=(host, port, user, password, rowqueue),
        kwargs={"limit": 100},
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
                validate_bucket_consumer,
                row,
                crepo_host,
                test_bucket,
                rowqueue,
            )
    rowqueue.join()


def validate_bucket_consumer(row, crepo_host, test_bucket, rowqueue):
    try:
        articlefile = AmbraFile(row, crepo_host, test_bucket)
        articlefile.get_crepo_data()
        blob = test_bucket.blob(articlefile.gcs_key)
        if blob.exists():
            blob.reload()
            if blob.content_type == articlefile.crepo_contentType:
                logger.info(f"OK - content types for {articlefile.gcs_key} match!")
            else:
                logger.warning(
                    f"Uh-oh - content type mismatch for {articlefile.gcs_key}!"
                )
    finally:
        rowqueue.task_done()


if __name__ == "__main__":
    main()
