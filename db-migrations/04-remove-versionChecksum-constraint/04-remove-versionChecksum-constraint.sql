
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
# Drop 'keySum' unique contraint from objects table (bucketId, objkey, versionChecksum)
# MySql does not support something like 'DROP CONSTRAINT', instead we use DROP INDEX
#

ALTER TABLE objects DROP INDEX keySum;

#
# Drop 'keySum' unique contraint from collections table (bucketId, objkey, versionChecksum)
#

ALTER TABLE collections DROP FOREIGN KEY keySum;

# INSERT the version string. This should happen last.
# The temporal order will indicate which scripts have been
# run to update this database.
INSERT CREPO_SCHEMA_INFO SET schema_ver = '04-remove-versionChecksum-constraint';
