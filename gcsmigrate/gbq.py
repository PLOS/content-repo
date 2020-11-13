from google.cloud import bigquery

schema = [
    bigquery.SchemaField("ambra_id", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("ambra_ingestion", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("ambra_crepokey", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("ambra_crepo_uuid", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("ambra_filesize", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("ambra_filename", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("ambra_bucket", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("crepo_found", "BOOL", mode="REQUIRED"),
    bigquery.SchemaField("gcs_found", "BOOL", mode="REQUIRED"),
    bigquery.SchemaField("crepo_host", "STRING"),
    bigquery.SchemaField("crepo_filesize", "INTEGER"),
    bigquery.SchemaField("crepo_checksum", "STRING"),
    bigquery.SchemaField("crepo_content_type", "STRING"),
    bigquery.SchemaField("crepo_filename", "STRING"),
    bigquery.SchemaField("crepo_version", "INTEGER"),
    bigquery.SchemaField("gcs_bucket", "STRING"),
    bigquery.SchemaField("gcs_key", "STRING"),
    bigquery.SchemaField("gcs_filesize", "INTEGER"),
    bigquery.SchemaField("gcs_checksum", "STRING"),
    bigquery.SchemaField("gcs_content_type", "STRING"),
]


def create_table(client, table_id):
    table = bigquery.Table(table_id, schema=schema)
    table = client.create_table(table)
    return table
