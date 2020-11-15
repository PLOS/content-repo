from google.cloud import bigquery

schema = [
    bigquery.SchemaField("ambra_fileId", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("ambra_bucketName", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("ambra_crepoKey", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("ambra_crepoUuid", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("ambra_fileSize", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("ambra_fileType", "STRING"),
    bigquery.SchemaField("ambra_ingestedFileName", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("ambra_ingestionId", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("ambra_ingestionNumber", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("ambra_articleId", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("ambra_doi", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("ambra_articleType", "STRING"),
    bigquery.SchemaField("crepo_found", "BOOL", mode="REQUIRED"),
    bigquery.SchemaField("crepo_host", "STRING"),
    bigquery.SchemaField("crepo_size", "INTEGER"),
    bigquery.SchemaField("crepo_checksum", "STRING"),
    bigquery.SchemaField("crepo_contentType", "STRING"),
    bigquery.SchemaField("crepo_downloadName", "STRING"),
    bigquery.SchemaField("crepo_versionNumber", "INTEGER"),
    bigquery.SchemaField("gcs_found", "BOOL", mode="REQUIRED"),
    bigquery.SchemaField("gcs_bucket", "STRING"),
    bigquery.SchemaField("gcs_key", "STRING"),
    bigquery.SchemaField("gcs_size", "INTEGER"),
    bigquery.SchemaField("gcs_checksum", "STRING"),
    bigquery.SchemaField("gcs_contentType", "STRING"),
]


def create_table(client, table_id):
    table = bigquery.Table(table_id, schema=schema)
    table = client.create_table(table)
    return table
