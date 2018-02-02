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

You will need to place one of the following Resources in Tomcat's context.xml :

If you are using **MogileFS**, set the domain and trackers accordingly. Trackers are comma separated.

    <Resource name="repo/objectStore"
        type="org.plos.repo.service.ObjectStore"
        factory="org.plos.repo.config.MogileStoreFactory"
        domain="toast"
        trackers="localhost:7001"
        maxTrackerConnections="1"
        maxIdleConnections="1"
        maxIdleTimeMillis="100" />
        
If you are storing data on a **local filesystem**, set the 'dataDirectory' accordingly. When you create the directory on your filesystem make sure it is owned by 'tomcat'.

    <Resource name="repo/objectStore"
        type="org.plos.repo.service.ObjectStore"
        factory="org.plos.repo.config.FileSystemStoreFactory"
        dataDirectory="/path/to/data/directory" />
        
Note: The local filesystem implementation can also be used to serve reproxied objects. You need to have something serve files out of the data directory as static files. One way to do this is to use Nginx, by placing something like this in your /etc/nginx/sites-enables/repo.conf:

    server {
        location /objdata/ {
            alias /path/to/data/directory;
        }
    }
    
Then when you configure the FileSystemStoreFactory, set the reproxyBaseUrl variable:

    <Resource name="repo/objectStore"
        type="org.plos.repo.service.ObjectStore"
        factory="org.plos.repo.config.FileSystemStoreFactory"
        dataDirectory="/path/to/data/directory"
        reproxyBaseUrl = "http://localhost/objdata/" />

Now you should be able to make object GET requests and ask for reproxied URLs.
        

If you are using **Amazon S3** as the object store, set your access key and secret key.

    <Resource name="repo/objectStore"
        type="org.plos.repo.service.S3StoreService"
        factory="org.plos.repo.config.S3StoreFactory"
        awsAccessKey="abc"
        awsSecretKey="def" />
        
For testing purposes there is also an **InMemoryFileStore** which you can simply use like so:

    <Resource name="repo/objectStore"
        type="org.plos.repo.service.ObjectStore"
        factory="org.plos.repo.config.InMemoryStoreFactory" />



Database Backends
-----------------

The service supports HSQLDB and MySQL. You will need to place one of the following Resources in Tomcat's context.xml :

If you are using **MySQL**, you need to manually create a database (I called it 'repo' in the example below but call it what you want) and create a user with granted write permissions. 

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

**HSQLDB** can support file and in memory databases. Set the 'url' to your configuration.

    <Resource name="jdbc/repoDB"
        auth="Container"
        type="javax.sql.DataSource"
        validationQuery="SELECT 1 FROM INFORMATION_SCHEMA.SYSTEM_USERS"
        driverClassName="org.hsqldb.jdbc.JDBCDriver"
        username=""
        password=""
        url="jdbc:hsqldb:file:/tmp/repo-hsqldb;shutdown=true" />

See the [Ambra Project documentation](https://plos.github.io/ambraproject/) for
an overview of the stack and user instructions. If you have any questions or
comments, please email dev@ambraproject.org, open a [GitHub
issue](https://github.com/PLOS/content-repo/issues), or submit a pull request.
