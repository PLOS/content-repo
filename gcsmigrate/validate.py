import argparse
import base64
from datetime import datetime
import logging
import queue
import requests
from concurrent.futures.thread import ThreadPoolExecutor
import threading

from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import sessionmaker

from google.cloud import storage, bigquery

from gbq import create_table


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)


rowqueue = queue.Queue(maxsize=1000)


def main():
    parser = argparse.ArgumentParser(description="verify GCS data from rhino")
    parser.add_argument("--host", "-H", help="mysql host")
    parser.add_argument("--port", "-P", help="mysql port")
    parser.add_argument("--user", "-u", help="mysql user")
    parser.add_argument("--password", "-p", help="mysql password")
    parser.add_argument("--crepo-host", "-C", help="crepo host name")
    parser.add_argument("--gcs-project", "-G", help="gcp project", default="plos-dev")
    parser.add_argument("--gcs-bucket", "-B", help="gcs bucket name")
    parser.add_argument("--gbq-dataset", "-D", help="BigQuery dataset name")
    parser.add_argument(
        "--gbq-project", "-Q", help="BigQuery project", default="plos-dev"
    )

    args = parser.parse_args()

    gcs = storage.Client(project=args.gcs_project)
    gbq = bigquery.Client(project=args.gbq_project)
    table_id = f"{args.gbq_project}.{args.gbq_dataset}.{datetime.now().strftime('%s')}"
    create_table(gbq, table_id)

    threading.Thread(
        target=enqueue,
        args=(args.host, args.port, args.user, args.password),
        daemon=True,
    ).start()

    # using a standard ThreadPoolExecutor here eventually leads to OOM
    with BoundedThreadPoolExecutor(max_workers=5) as executor:
        while True:
            row = rowqueue.get()
            if row == "DONE":
                rowqueue.join()
                break
            executor.submit(
                process_articleFile,
                row,
                args.gcs_bucket,
                args.crepo_host,
                gcs,
                gbq,
                table_id,
            )


def enqueue(host, port, user, password):
    engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/ambra")
    metadata = MetaData(engine)
    mksession = sessionmaker(bind=engine)
    session = mksession()

    articleFile = Table("articleFile", metadata, autoload=True)
    articleIngestion = Table("articleIngestion", metadata, autoload=True)
    article = Table("article", metadata, autoload=True)

    for row in (
        session.query(articleFile, articleIngestion, article)
        .join(
            articleIngestion,
            articleIngestion.columns.ingestionId == articleFile.columns.ingestionId,
        )
        .join(
            article,
            article.columns.articleId == articleIngestion.columns.articleId,
        )
        .yield_per(1000)
    ):
        rowqueue.put(row._asdict())
    rowqueue.put("DONE")


def process_articleFile(row, bucket, crepo_host, gcs_client, gbq_client, gbq_table):
    articlefile = AmbraFile(row, crepo_host)
    articlefile.gcs_bucket = bucket
    url, params = articlefile.crepo_url
    response = requests.get(url, params=params)
    if response.status_code == 200:
        logger.debug(
            f"found crepo object {articlefile.ambra_crepoKey} in {articlefile.ambra_bucketName}"
        )
        crepo_object = response.json()
        articlefile.crepo_found = True
        for key, value in crepo_object.items():
            if key in [
                "size",
                "checksum",
                "contentType",
                "downloadName",
                "versionNumber",
            ]:
                key = f"crepo_{key}"
                setattr(articlefile, key, value)
    else:
        logger.warning(f"no crepo object {row.crepoKey} in {row.bucketName}")
    try:
        bucket = gcs_client.bucket(articlefile.gcs_bucket)
        path = articlefile.gcs_key
        blob = bucket.blob(path)
        if blob.exists():
            logger.debug(f"found {articlefile.gcs_key} in {articlefile.gcs_bucket}")
            articlefile.gcs_found = True
            blob.reload()
            articlefile.gcs_checksum = base64.b64decode(blob.md5_hash).hex()
            articlefile.gcs_size = blob.size
            articlefile.gcs_contentType = blob.content_type
        else:
            logger.warning(
                f"blob not found: {articlefile.gcs_key} in {articlefile.gcs_bucket}"
            )
    except Exception as ex:
        logger.error(ex)
    try:
        errors = gbq_client.insert_rows_json(gbq_table, [articlefile.serialize()])
        if errors:
            logger.error(f"errors inserting into GBQ: {errors}")
    except Exception as ex:
        logger.error(f"{ex}")
    rowqueue.task_done()


class AmbraFile:
    def __init__(self, ambra_data, crepo_host):
        for key, value in ambra_data.items():
            if key in [
                "articleId",
                "articleType",
                "bucketName",
                "crepoKey",
                "crepoUuid",
                "doi",
                "fileId",
                "fileSize",
                "fileType",
                "ingestedFileName",
                "ingestionId",
                "ingestionNumber",
            ]:
                key = f"ambra_{key}"
                setattr(self, key, value)
        self.crepo_found = False
        self.crepo_host = crepo_host
        self.crepo_size = None
        self.crepo_checksum = None
        self.crepo_contentType = None
        self.crepo_downloadName = None
        self.crepo_versionNumber = None
        self.gcs_found = False
        self.gcs_bucket = None
        self.gcs_size = None
        self.gcs_checksum = None
        self.gcs_contentType = None

    def serialize(self):
        serialized = {}
        for key, value in self.__dict__.items():
            if (
                key.startswith("ambra")
                or key.startswith("crepo")
                or key.startswith("gcs")
            ):
                if value is not None:
                    serialized[key] = value
        serialized["gcs_key"] = self.gcs_key
        return serialized

    @property
    def crepo_url(self):
        url = f"http://{self.crepo_host}:8002/v1/objects/{self.ambra_bucketName}"
        params = {
            "key": self.ambra_crepoKey,
            "uuid": self.ambra_crepoUuid,
            "fetchMetadata": True,
        }
        return url, params

    @property
    def gcs_key(self):
        return f"{self.ambra_doi}/{self.ambra_ingestionNumber}/{self.ambra_ingestedFileName}"


class BoundedThreadPoolExecutor(ThreadPoolExecutor):
    semaphore = None

    def __init__(self, max_workers=None):
        super().__init__(max_workers)
        self.semaphore = threading.BoundedSemaphore(max_workers)

    def acquire(self):
        self.semaphore.acquire()

    def release(self, fn):
        self.semaphore.release()

    def submit(self, fn, *args, **kwargs):
        self.acquire()
        future = super().submit(fn, *args, **kwargs)
        future.add_done_callback(self.release)
        return future


if __name__ == "__main__":
    main()
