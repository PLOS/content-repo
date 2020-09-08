# Migrating from mogile to GCS

This folder contains code for migrating from Mogile to GCS.

In broad strokes, the migration from mogile to GCS consists of the following steps:

1. Build and configure the migration.
1. Sync data from mogile to an GCS bucket
1. Remove or fix bad data.
1. Run a large-scale migration in a cloud function.
1. Stop ingesting new data into contentrepo.
1. Run a large-scale migration in a cloud function.
1. Verify migration.
1. Update contentrepo configuration.
1. Update data in contentrepo and rhino databases to point to the new, GCS buckets.
1. Done!
1. Test
1. Re-enable ingest.


## Details

### Build and configure

1. Configure a `.env` file for use in the commands below. It should contain:
- `GOOGLE_APPLICATION_CREDENTIALS` pointing to the location of your downloaded JSON credentials
- `GCP_PROJECT` your GCP project
- `TOPIC_ID` the pubsub topic id
- `MOGILE_DATABASE_URL` pointing at the mogile database to use
- `MOGILE_TRACKERS` a comma-separated list of mogile trackers, e.g. `mogile-1.domain:7001,mogile-2.domain:7001`
- `BUCKETS` a comma separated list of colon based mappings from old bucket names to new ones. For example: `old-bucket:new-bucket,old-bucket2:new-bucket2`. There must be a mapping for every old bucket in mogile that contains data.

### Remove or fix bad data

All objects with a dkey of the form `{UUID}.tmp` stored in mogile should be removed. These are temporary files left over from failed contentrepo ingests.

These can be identified in the mogile database using:
```
> select dkey from file where dkey like "%.tmp";
```

The files can be delete from mogile using the python client:
```
pipenv run python
>>> c = pymogilefs.client.Client( trackers=os.environ['MOGILE_TRACKERS'].split(','), domain='plos_repo')
>>> c.delete_file(DKEY)
```
for each `DKEY` identified in the previous step.

All files with known bad checksums stored in mogile using the wrong content addressable storage should be fixed. There is only one known instance of this.

```
UPDATE file SET dkey='f79ba8f5b9fecbeb8ace12421c73d5198bea6d05-mogilefs-prod-repo' where dmid = 1 and dkey = 'acec11893a476ca42e95c8d546431202ea821210-mogilefs-prod-repo';
```

### Run a large-scale migration in a cloud function

The following command should be run:
```
pipenv run python enqueue.py migrate
```
This will enqueue all items in mogile to a pubsub topic for processing. These jobs can be monitored using the GCP console.

### Stop ingesting new data into contentrepo

This should happen just before we are ready to switch over to GCS.

### Run a large-scale migration in a cloud function

Rerun the following command:
```
pipenv run python enqueue.py migrate
```

This should only take a few hours, as the vast majority of the items should have been migrated over in a previous step. When the jobs have all succeeded successfully, move on.

### Verify migration

First, verify that the total number of items in the firebase collection matches the total number of items in mogile. This can be done by running:

```
$ pipenv run python fids.py > fids.list
$ wc -l fids.list
```

and compare with (mysql on mogile)
```
> SELECT COUNT(fid) from file;
```

If these counts do not match, there is a problem.

Next, we can run the GCP checksum verification:
```
pipenv run python enqueue.py verify
```

All cloud function jobs should finish without error.

Next, we can run the local verify script:

```
pipenv run python verify.py
```

The logic in this is slightly different to double-check the cloud function verification. Also, it runs completely locally. It should complete without error.

### Update contentrepo and lemur configuration.

Now we will want to update the contentrepo and lemur configuration. (Details to be worked out.)

### Update data in contentrepo and rhino databases to point to the new, GCS buckets.

Bucket names are stored in both the contentrepo and rhino database. We need to update them with the new repo names.

In contentrepo, run:
```
> UPDATE buckets SET bucketName='NEW_BUCKET_NAME' WHERE bucketName = 'OLD_BUCKET_NAME';
```
for each bucket.

In rhino, run:

```
> DROP INDEX bucketName_index on articleFile;
> UPDATE articleFile SET bucketName='NEW_BUCKET_NAME' WHERE bucketName = 'OLD_BUCKET_NAME';
CREATE INDEX bucketName_index on articleFile (bucketName);
```

for the corpus bucket only. It is necessary to drop the index first and recreate to avoid very slow performance when updating around 10 million rows in the database.


### Test

QA to perform tests on the newly configured GCS backend.

### Re-enable ingest

We should be able to ingest into the new bucket now.

### Deploying

```
gcloud functions deploy main --region us-east1 --runtime python37 --trigger-topic=corpus-migration --project=plos-dev --vpc-connector=projects/plos-dev/locations/us-east1/connectors/plos-dev-vpc-acc-1 --max-instances=100 --env-vars-file=dev.env.yaml --timeout=240s
```
