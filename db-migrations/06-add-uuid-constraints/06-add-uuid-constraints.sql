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
# This script must be execute after the backfill of uuids for existing objects and collections
#

#
# Add not null constraint to uuid column from Objects table, and
# add unique contraint for bucketId, objkey and uuid
#

ALTER TABLE objects MODIFY uuid BINARY(16) NOT NULL;
ALTER TABLE objects 
ADD CONSTRAINT ObjkeyUniversalID UNIQUE (bucketId, objkey, uuid);

#
# Add not null constraint to uuid column from Collections table, and
# add unique contraint for bucketId, collKey and uuid
#

ALTER TABLE collections MODIFY uuid BINARY(16) NOT NULL;
ALTER TABLE collections 
ADD CONSTRAINT CollkeyUniversalID UNIQUE (bucketId, collkey, uuid);

# INSERT the version string. This should happen last.
# The temporal order will indicate which scripts have been
# run to update this database.
INSERT CREPO_SCHEMA_INFO SET schema_ver = '06-add-uuid-constraints';