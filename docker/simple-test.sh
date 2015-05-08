#!/bin/bash

# ping the service
curl localhost:8085/config

# check if mogile node is up
docker exec -it docker_tracker_1 curl http://node:7500/dev1

docker exec -it docker_tracker_1 curl http://node:7500/dev1/usage

# mogile upload
docker exec -it docker_tracker_1 mogupload --trackers=localhost --domain=maindomain --key=hosts --file=/etc/hosts 

# create a bucket
BUCKET=bucket_`date +%N`

echo bucket: $BUCKET

curl --data "name=$BUCKET"  http://localhost:8085/buckets

# create an object
OBJECT=object_`date +%N`

curl -F "create=new" -F "key=$OBJECT" -F "bucketName=$BUCKET" -F "file=@`pwd`/README.md" http://localhost:8085/objects

# read it back
curl -I http://localhost:8085/objects/$BUCKET?key=$OBJECT

