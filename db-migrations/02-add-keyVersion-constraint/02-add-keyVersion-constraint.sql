/*
 * Copyright (c) 2014-2019 Public Library of Science
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
 * THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 */


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
# Before to create the unique contraint, is important to validate the data and fix the duplicate information if it exists.
# We can found duplicate information with the following select statements in objects and collections tables respectively:
#
# SELECT id, bucketId, objkey, versionNumber FROM objects GROUP BY bucketId, objkey, versionNumber
#    HAVING count(*) > 1 ORDER BY objkey,versionNumber;
# SELECT id, bucketId, collkey, versionNumber FROM collections GROUP BY bucketId, collkey, versionNumber
#    HAVING count(*) > 1 ORDER BY collkey,versionNumber;
#

#
# Create the unique constraint to ensure no duplicate values in columns: bucketId, objKey and versionNumber
#

ALTER TABLE objects ADD CONSTRAINT keyVersion UNIQUE (bucketId, objkey, versionNumber);

#
# Create the unique constraint to ensure no duplicate values in columns: bucketId, collkey and versionNumber
#

ALTER TABLE collections ADD CONSTRAINT keyVersion UNIQUE (bucketId, collkey, versionNumber);

# INSERT the version string. This should happen last.
# The temporal order will indicate which scripts have been
# run to update this database.
INSERT CREPO_SCHEMA_INFO SET schema_ver = '02-add-keyVersion-constraint';
