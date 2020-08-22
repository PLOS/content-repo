import os

import dj_database_url
import pymysql
from tqdm import tqdm


def make_db_connection(db_url):
    config = dj_database_url.parse(db_url)
    return pymysql.connect(
        host=config["HOST"],
        user=config["USER"],
        password=config["PASSWORD"],
        db=config["NAME"],
        cursorclass=pymysql.cursors.SSCursor,
    )


def process_rows(db_url, sql, uuid_dict):
    connection = make_db_connection(db_url)
    try:
        cursor = connection.cursor()
        cursor.execute(sql)
        with tqdm(desc=sql) as pbar:
            row = cursor.fetchone()
            pbar.update()
            while row:
                (key, uuid, size) = row
                uuid_dict[uuid] = (key, size)
                row = cursor.fetchone()
                pbar.update()
    finally:
        connection.close()


rhino_keys = dict()
process_rows(
    os.environ["RHINO_DATABASE_URL"],
    "select crepoKey, crepoUuid, fileSize from articleFile;",
    rhino_keys,
)

contentrepo_keys = dict()
process_rows(
    os.environ["CONTENTREPO_DATABASE_URL"],
    # preprints or corpus bucket
    "select objKey, uuid, size from objects where bucketId IN (1, 9, 2112);",
    contentrepo_keys,
)


def check_equal(uuid):
    rhino = rhino_keys[uuid]
    contentrepo = contentrepo_keys[uuid]
    assert (
        rhino == contentrepo
    ), f"{key}: rhino ({rhino}) != contentrepo ({contentrepo})"


for uuid in rhino_keys.keys():
    check_equal(uuid)
