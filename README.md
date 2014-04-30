Content Repository
==================

The content repository is a general use data store which functions similarly to Amazon S3.

While it is agnostic of what data is stored, all stored items are called 'objects'. Storage locations are called 'buckets'. A single object can have multiple versions within a bucket.


REST API
--------

All interactions with the repository should be performed through its REST API which supports both JSON and XML via content negotiation. The interface uses Jersey and the API exposes its own usage documentation via Swagger.

Once the service is running in a Tomcat container, visit the web root to see the documentation (for example [http://localhost:8080/](http://localhost:8080/)).


Storage Backends
----------------

The service supports multiple object store backend implementations which can be specified at service initialization time; after the first object is stored the backend cannot be changed. At the time of this writing it supports the following backends: Local filesystem, MogileFS, Amazon S3.

You will need to place one of the following Resources in Tomcat's config.xml :

    <Resource name="repo/objectStore"
        type="org.plos.repo.service.ObjectStore"
        factory="org.plos.repo.config.FileSystemStoreFactory"
        dataDirectory="/path/to/data/directory" />

    <Resource name="repo/objectStore"
        type="org.plos.repo.service.S3StoreService"
        factory="org.plos.repo.config.S3StoreFactory"
        awsAccessKey="abc"
        awsSecretKey="def" />

    <Resource name="repo/objectStore"
        type="org.plos.repo.service.ObjectStore"
        factory="org.plos.repo.config.MogileStoreFactory"
        domain="toast"
        trackers="localhost:7001"
        maxTrackerConnections="1"
        maxIdleConnections="1"
        maxIdleTimeMillis="100" />


Database Backends
-----------------

The service supports HSQLDB and MySQL.

You will need to place one of the following Resources in Tomcat's config.xml :

    <Resource name="jdbc/repoDB"
        auth="Container"
        type="javax.sql.DataSource"
        validationQuery="SELECT 1 FROM INFORMATION_SCHEMA.SYSTEM_USERS"
        driverClassName="org.hsqldb.jdbc.JDBCDriver"
        username=""
        password=""
        url="jdbc:hsqldb:file:/tmp/repo-hsqldb;shutdown=true" />

    <Resource name="jdbc/repoDB"
        auth="Container"
        type="javax.sql.DataSource"
        factory="org.apache.tomcat.jdbc.pool.DataSourceFactory"
        validationQuery="SELECT 1"
        testOnBorrow="true"
        driverClassName="com.mysql.jdbc.Driver"
        username="root"
        password=""
        url="jdbc:mysql://localhost:3306/repo" />
