# Migrating from mogile to GCS

This folder contains code for migrating from Mogile to GCS.

## Details

### Build and configure

1. Configure a `.env` file for use in the commands below. It should contain:
- `GOOGLE_APPLICATION_CREDENTIALS` pointing to the location of your downloaded
  JSON credentials
- `GCP_PROJECT` your GCP project
- `TOPIC_ID` the pubsub topic id
- `MOGILE_DATABASE_URL` pointing at the mogile database to use
- `MOGILE_TRACKERS` a comma-separated list of mogile trackers, e.g.
  `mogile-1.domain:7001,mogile-2.domain:7001`
- `BUCKETS` a comma separated list of colon based mappings from old bucket names
  to new ones. For example: `old-bucket:new-bucket,old-bucket2:new-bucket2`.
- There must be a mapping for every old bucket in mogile that contains data.

- `IGNORE_BUCKETS` a comma separated list of buckets that do not need to be migrated

### Deploying

```
gcloud functions deploy main --region us-east1 --runtime python37 \
 --trigger-topic=corpus-migration --project=plos-dev \
 --vpc-connector=projects/plos-dev/locations/us-east1/connectors/plos-dev-vpc-acc-1 \ 
 --max-instances=100 --env-vars-file=dev.env.yaml --timeout=360s --memory=1024MB
```

### Validating

Run `validate.py` to generate BigQuery data for validation analysis.

```
pipenv run python validate.py -H journals-prod1-db1.soma.plos.org -P 3306 \
 -u ambra_ro -p ******** -C contentrepo-102.soma.plos.org \
 -B corpus-dev-0242ac130003 -D articleValidation -L debug
```

This will create a timestamped table in the `articleValidation` dataset in
BigQuery, which can be used to analyze content migration status.


### Fixing content types

Run `fix_gcs_content_types.py` to update GCS objects with the correct
content-type from crepo:

```
pipenv run python fix_gcs_content_types.py -H journals-prod1-db2.soma.plos.org \
 -P 3306 -u ambra_ro -p ******** -C contentrepo-102.soma.plos.org \ 
 -B  corpus-dev-0242ac130003 -L debug
```

To test this functionality, run `test_fix_gcs_content_types.py`, which copies
objects into a new test bucket, runs the fix against them, and reports the
results.

```
python test_fix_gcs_content_types.py -H journals-prod1-db2.soma.plos.org \
 -P 3306 -u ambra_ro -p ******* -C contentrepo-102.soma.plos.org \
 -B corpus-dev-0242ac130003 -L info
```
