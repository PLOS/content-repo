#
# Start tracking the Content Repo Schema Versions
# The string in schema_ver will indicate the last
# migration script that was executed in the database.
# New versions are added with INSERT so an audit
# trail of migration scripts will be created in
# temporal ordering.
#

#
CREATE TABLE IF NOT EXISTS CREPO_SCHEMA_INFO (
    timestamp timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    schema_ver VARCHAR (100) NOT NULL
);

#
# Create the table representing Audit.
#
CREATE TABLE IF NOT EXISTS audit (
    id INTEGER NOT NULL AUTO_INCREMENT,
    bucketName VARCHAR (255) NOT NULL,
    keyValue VARCHAR (255),
    operation VARCHAR (20) NOT NULL,
    uuid CHAR (36),
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id)
);

#
# Objects are being update with UUID information
#

ALTER TABLE objects
ADD COLUMN uuid CHAR(36);

#
# Collections are being update with UUID information
#

ALTER TABLE collections
ADD COLUMN uuid CHAR(36);

DELIMITER //

DROP PROCEDURE IF EXISTS backFillObjectUUID//
CREATE PROCEDURE `backFillObjectUUID` ()

BEGIN
    DECLARE v_finished INTEGER DEFAULT 0;
    DECLARE ts_ TIMESTAMP;
    DECLARE id_ INT;
    DEClARE ts_cursor CURSOR FOR 
       SELECT id, timestamp FROM objects where uuid IS NULL;

    DECLARE CONTINUE HANDLER 
       FOR NOT FOUND SET v_finished = 1;
    
    OPEN ts_cursor;
    update_cDate: LOOP
        FETCH ts_cursor into id_, ts_;

        IF v_finished = 1 THEN 
           LEAVE update_cDate;
        END IF;
      
        UPDATE objects SET uuid=UUID(), timestamp=ts_ where id = id_;

    END LOOP update_cDate;
    CLOSE ts_cursor;
END //


DROP PROCEDURE IF EXISTS backFillCollectionUUID//
CREATE PROCEDURE `backFillCollectionUUID` ()

BEGIN
    DECLARE v_finished INTEGER DEFAULT 0;
    DECLARE ts_ TIMESTAMP;
    DECLARE id_ INT;
    DEClARE ts_cursor CURSOR FOR 
       SELECT id, timestamp FROM collections where uuid IS NULL;

    DECLARE CONTINUE HANDLER 
       FOR NOT FOUND SET v_finished = 1;
    
    OPEN ts_cursor;
    update_cDate: LOOP
        FETCH ts_cursor into id_, ts_;

        IF v_finished = 1 THEN 
           LEAVE update_cDate;
        END IF;   
        UPDATE collections SET uuid=UUID(), timestamp=ts_ where id = id_;

    END LOOP update_cDate;
    CLOSE ts_cursor;
END //
DELIMITER ;

CALL backFillObjectUUID();
CALL backFillCollectionUUID();

#
# Comments that start with #Middle are percona pt-online-schema-change commands
# That can be substituted for SQL Commands between HERE and #DEPLOY on the
# command line.
#
DROP index keySum on objects;
ALTER TABLE objects MODIFY versionChecksum varchar(255);

DROP index keySum on collections;
ALTER TABLE collections MODIFY versionChecksum varchar(255);

ALTER TABLE objects ADD CONSTRAINT UNIQUE ObjkeyUniversalID (bucketId, objkey, uuid);
ALTER TABLE objects ADD CONSTRAINT UNIQUE keyVersion (bucketId, objkey, versionNumber);

ALTER TABLE collections ADD CONSTRAINT UNIQUE CollkeyUniversalID (bucketId, collkey, uuid);
ALTER TABLE collections ADD CONSTRAINT UNIQUE keyVersion (bucketId, collkey, versionNumber);

INSERT CREPO_SCHEMA_INFO SET schema_ver = '04-UUID-audit-upgrade-predeploy';
