import argparse
from datetime import datetime
import queue
import requests
from concurrent.futures.thread import ThreadPoolExecutor
import threading

from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import sessionmaker

from google.cloud import storage, bigquery

from gbq import create_table


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

    gbq = bigquery.Client(project=args.gbq_project)
    table_id = f"{args.gbq_project}.{args.gbq_dataset}.{datetime.now().strftime('%s')}"
    create_table(gbq, table_id)

    threading.Thread(
        target=enqueue,
        args=(args.host, args.port, args.user, args.password),
        daemon=True,
    ).start()

    with ThreadPoolExecutor(max_workers=5) as executor:
        while True:
            row = rowqueue.get()
            if row == "DONE":
                break
            executor.submit(
                process_articleFile,
                row,
                args.gcs_project,
                args.gcs_bucket,
                args.crepo_host,
                gbq,
                table_id,
            )
            rowqueue.task_done()


def enqueue(host, port, user, password):
    engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/ambra")
    metadata = MetaData(engine)
    ArticleFile = Table("articleFile", metadata, autoload=True)
    mksession = sessionmaker(bind=engine)
    session = mksession()
    for row in session.query(ArticleFile).yield_per(1000):
        rowqueue.put(row)
    rowqueue.put("DONE")


def process_articleFile(row, project, bucket, crepo_host, gbq_client, gbq_table):
    ambra_file = AmbraFile(
        row.fileId,
        row.ingestionId,
        row.crepoKey,
        row.crepoUuid,
        row.fileSize,
        row.ingestedFileName,
        row.bucketName,
        crepo_host,
    )
    url, params = ambra_file.crepo_url
    response = requests.get(url, params=params)
    if response.status_code == 200:
        print(f"found crepo object {row.crepoKey} in {row.bucketName}")
        crepo_object = response.json()
        ambra_file.crepo_found = True
        ambra_file.crepo_filesize = crepo_object["size"]
        ambra_file.crepo_checksum = crepo_object["checksum"]
        ambra_file.crepo_content_type = crepo_object["contentType"]
        ambra_file.crepo_filename = crepo_object["downloadName"]
        ambra_file.crepo_version = crepo_object["versionNumber"]
    else:
        print(f"no crepo object {row.crepoKey} in {row.bucketName}")
    try:
        gcs = storage.Client(project=project)
        ambra_file.gcs_bucket = bucket
        bucket = gcs.bucket(ambra_file.gcs_bucket)
        path = ambra_file.gcs_key
        blob = bucket.blob(path)
        if blob.exists():
            print(f"found {ambra_file.gcs_key} in {ambra_file.gcs_bucket}")
            ambra_file.gcs_found = True
            blob.reload()
            ambra_file.gcs_checksum = blob.md5_hash
            ambra_file.gcs_filesize = blob.size
            ambra_file.gcs_content_type = blob.content_type
        else:
            print(f"blob not found: {ambra_file.gcs_key} in {ambra_file.gcs_bucket}")
    except Exception as ex:
        print(ex)
    try:
        errors = gbq_client.insert_rows_json(gbq_table, [ambra_file.serialize()])
        if errors:
            print(f"errors inserting into GBQ: {errors}")
    except Exception as ex:
        print(f"{ex}")
    return


class AmbraFile:
    def __init__(
        self,
        ambra_id,
        ambra_ingestion,
        ambra_crepokey,
        ambra_crepo_uuid,
        ambra_filesize,
        ambra_filename,
        ambra_bucket,
        crepo_host,
    ):
        self.ambra_id = ambra_id
        self.ambra_ingestion = ambra_ingestion
        self.ambra_crepokey = ambra_crepokey
        self.ambra_crepo_uuid = ambra_crepo_uuid
        self.ambra_filesize = ambra_filesize
        self.ambra_filename = ambra_filename
        self.ambra_bucket = ambra_bucket
        self.crepo_host = crepo_host
        self.crepo_filesize = None
        self.crepo_checksum = None
        self.crepo_content_type = None
        self.crepo_filename = None
        self.crepo_version = None
        self.crepo_found = False
        self.gcs_bucket = None
        self.gcs_filesize = None
        self.gcs_checksum = None
        self.gcs_content_type = None
        self.gcs_found = False

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
        url = f"http://{self.crepo_host}:8002/v1/objects/{self.ambra_bucket}"
        params = {
            "key": self.ambra_crepokey,
            "uuid": self.ambra_crepo_uuid,
            "fetchMetadata": True,
        }
        return url, params

    @property
    def gcs_key(self):
        return f"{self.ambra_crepokey}/{self.ambra_ingestion}/{self.ambra_filename}"


if __name__ == "__main__":
    main()
