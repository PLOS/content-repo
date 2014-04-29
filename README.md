The content repository is a general use data store which functions similarly to Amazon S3.

While it is agnostic of what data is stored, all stored items are called 'objects'. Storage locations are called 'buckets'. A single object can have multiple versions within a bucket.

All interactions with the repository should be performed through its REST API which supports both JSON and XML via content negotiation. The interface uses Jersey and the API exposes its own usage documentation via Swagger.

Once the service is running in a Tomcat container, visit the root to see the documentation (for example http://localhost:8080/).

The service supports the following storage backends: Local filesystem, MofileFS, Amazon S3
For a database it supports: MySql, HSQLdb

The database and storage backend should be specified in your Tomcat context.xml file.

