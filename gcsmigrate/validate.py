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
                table_id,
                gbq,
                rowqueue,
            )


def enqueue(host, port, user, password, rowqueue):
    engine = create_engine(
        f"mysql://{user}:{password}@{host}:{port}/ambra", pool_recycle=5
    )
    engine.execute("SET wait_timeout=30")
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
        .execution_options(stream_results=True)
        .yield_per(1000)
    ):
        rowqueue.put(row._asdict())
    rowqueue.put("DONE")


def process_articleFile(row, crepo_host, gcs_bucket, gcs, gbq_table, gbq, rowqueue):
    articlefile = AmbraFile(row, crepo_host, gcs_bucket)
    articlefile.get_crepo_data()
    if articlefile.crepo_found:
        articlefile.get_gcs_data(gcs)
        articlefile.save_to_gbq(gbq, gbq_table)
    rowqueue.task_done()


class AmbraFile:
    def __init__(self, ambra_data, crepo_host, gcs_bucket):
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
        self.gcs_bucket = gcs_bucket
        self.gcs_size = None
        self.gcs_checksum = None
        self.gcs_contentType = None

    def get_crepo_data(self):
        url, params = self.crepo_url
        response = requests.get(url, params=params)
        if response.status_code == 200:
            logger.debug(
                f"found crepo object {self.ambra_crepoKey} in {self.ambra_bucketName}"
            )
            crepo_object = response.json()
            self.crepo_found = True
            for key, value in crepo_object.items():
                if key in [
                    "size",
                    "checksum",
                    "contentType",
                    "downloadName",
                    "versionNumber",
                ]:
                    key = f"crepo_{key}"
                    setattr(self, key, value)
        else:
            logger.warning(
                f"no crepo object {self.ambra_crepoKey} in {self.ambra_bucketName}"
            )

    def get_gcs_blob(self, gcs_client):
        try:
            bucket = gcs_client.bucket(self.gcs_bucket)
            path = self.gcs_key
            blob = bucket.blob(path)
            if blob.exists():
                logger.debug(f"found {self.gcs_key} in {self.gcs_bucket}")
                self.gcs_found = True
                return blob
        except Exception as ex:
            logger.warning(f"blob not found: {self.gcs_key} in {self.gcs_bucket}")

    def get_gcs_data(self, gcs_client):
        blob = self.get_gcs_blob(gcs_client)
        try:
            blob.reload()
            self.gcs_checksum = base64.b64decode(blob.md5_hash).hex()
            self.gcs_size = blob.size
            self.gcs_contentType = blob.content_type
        except Exception as ex:
            logger.error(ex)

    def save_to_gbq(self, gbq_client, gbq_table):
        try:
            errors = gbq_client.insert_rows_json(gbq_table, [self.serialize()])
            if errors:
                logger.error(f"errors inserting into GBQ: {errors}")
        except Exception as ex:
            logger.error(f"{ex}")

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
