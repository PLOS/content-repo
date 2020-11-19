# Migrating from mogile to GCS

This folder contains code for migrating from Mogile to GCS.

## Details

### Build and configure

1. Configure a `.env` file for use in the commands below. It should contain:
- `BUCKETS` a comma separated list of colon based mappings from old bucket names to new ones. For example: `old-bucket:new-bucket,old-bucket2:new-bucket2`. There must be a mapping for every old bucket in mogile that contains data.
- `COLLECTION_NAME` the firestore database to store results in
- `CONTENTREPO_DATABASE_URL` database URL for contentrepo
- `GCP_PROJECT` your GCP project
- `GOOGLE_APPLICATION_CREDENTIALS` pointing to the location of your downloaded JSON credentials
- `IGNORE_BUCKETS` a comma separated list of buckets that do not need to be migrated
- `MOGILE_DATABASE_URL` pointing at the mogile database to use
- `MOGILE_TRACKERS` a comma-separated list of mogile trackers, e.g. `mogile-1.domain:7001,mogile-2.domain:7001`
- `RHINO_DATABASE_URL` database URL for rhino
- `TOPIC_ID` the pubsub topic id

### Deploying

```
gcloud functions deploy main --region us-east1 --runtime python37 --trigger-topic=corpus-migration --project=plos-dev --vpc-connector=projects/plos-dev/locations/us-east1/connectors/plos-dev-vpc-acc-1 --max-instances=100 --env-vars-file=dev.env.yaml --timeout=360s --memory=1024MB
```
### How it works

See https://confluence.plos.org/confluence/display/ENG/Journals+GCP+Migration for more information.
