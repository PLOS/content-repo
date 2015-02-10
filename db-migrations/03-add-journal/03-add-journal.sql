
#
# Start tracking the Content Repo Schema Versions
# The string in schema_ver will indicate the last
# migration script that was executed in the database.
# New versions are added with INSERT so an audit
# trail of migration scripts will be created in
# temporal ordering.
#
CREATE TABLE IF NOT EXISTS CREPO_SCHEMA_INFO (
    timestamp timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    schema_ver VARCHAR (100) NOT NULL
);

#
# Create the table representing Journal.
#
CREATE TABLE IF NOT EXISTS journal (
    id INTEGER NOT NULL AUTO_INCREMENT,
    bucketName VARCHAR (255) NOT NULL,
    objkey VARCHAR (255),
    collKey VARCHAR (255),
    operation VARCHAR (10),
    versionChecksum VARCHAR (255),
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id)
);


# INSERT the version string. This should happen last.
# The temporal order will indicate which scripts have been
# run to update this database.
INSERT CREPO_SCHEMA_INFO SET schema_ver = '02-add-journal';
