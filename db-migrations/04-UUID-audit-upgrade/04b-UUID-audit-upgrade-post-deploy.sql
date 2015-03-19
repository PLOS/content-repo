#
# Start tracking the Content Repo Schema Versions
# The string in schema_ver will indicate the last
# migration script that was executed in the database.
# New versions are added with INSERT so an audit
# trail of migration scripts will be created in
# temporal ordering.
#

ALTER TABLE objects MODIFY uuid char(36) NOT NULL;
ALTER TABLE collections MODIFY uuid char(36) NOT NULL;
ALTER TABLE objects DROP COLUMN versionChecksum;
ALTER TABLE collections DROP COLUMN versionChecksum;

INSERT CREPO_SCHEMA_INFO SET schema_ver = '04-UUID-audit-upgrade-postdeploy';
