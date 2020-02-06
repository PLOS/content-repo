Content Repository
==================


build: [![Build Status Badge]][Build Status]&#8193;&#9733;&#8193;
integration: [![Integration Status Badge]][Integration Status]

The content repository is a general use data store which functions similarly to Amazon S3. 

While it is agnostic of what data is stored, all stored items are called 'objects'. Storage locations are called 'buckets'. A single object can have multiple versions within a bucket.


REST API
--------

All interactions with the repository should be performed through its REST API which supports both JSON and XML via content negotiation. The interface uses Jersey and the API exposes its own usage documentation via Swagger.

Once the service is running in a Tomcat container, visit the web root to see the documentation (for example [http://localhost:8080/](http://localhost:8080/)).


Storage Backends
----------------

The service supports multiple object store backend implementations which can be specified at service initialization time; after the first object is stored the backend cannot be changed. At the time of this writing it supports the following backends: Local filesystem, MogileFS, Amazon S3.

Environment variables are used for the configuration of the storage backend.

If you wish to use **MogileFS**, set the `MOGILE_TRACKERS` environment variable. For example:

```
MOGILE_TRACKERS=localhost:7001,otherhost:7001 mvn cargo:run
```

If you are storing data on a **local filesystem**, set the 'DATA_DIRECTORY' environment variable accordingly. When you create the directory on your filesystem make sure it is owned by 'tomcat'.

```
DATA_DIRECTORY=/path/to/data/directory mvn cargo:run
```        

If you are using **Amazon S3** as the object store, set your access key and secret key.

```
AWS_ACCESS_KEY_ID=FOO AWS_SECRET_ACCESS_KEY= mvn cargo:run
```

Database Backends
-----------------

The service supports MySQL. You will need to place one of the following Resources in Tomcat's context.xml :

To configure the database, use the `DATABASE_URL` environment variable.

```
DATABASE_URL="jdbc:mysql://localhost:3306/repo?user=root&password=password" mvn cargo:run
```

See the [Ambra Project documentation](https://plos.github.io/ambraproject/) for
an overview of the stack and user instructions. If you have any questions or
comments, please email dev@ambraproject.org, open a [GitHub
issue](https://github.com/PLOS/content-repo/issues), or submit a pull request.

[Build Status]: https://teamcity.plos.org/teamcity/viewType.html?buildTypeId=CRepo_Build
[Build Status Badge]: https://teamcity.plos.org/teamcity/app/rest/builds/buildType:(id:CRepo_Build)/statusIcon.svg
[Integration Status]: https://teamcity.plos.org/teamcity/viewType.html?buildTypeId=IntegrationTests_CRepoDev
[Integration Status Badge]: https://teamcity.plos.org/teamcity/app/rest/builds/buildType:(id:IntegrationTests_CRepoDev)/statusIcon.svg
