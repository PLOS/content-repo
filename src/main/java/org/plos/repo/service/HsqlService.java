package org.plos.repo.service;

import org.plos.repo.models.Bucket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.support.rowset.SqlRowSet;

import java.sql.Types;

public class HsqlService extends SqlService {

  private static final Logger log = LoggerFactory.getLogger(HsqlService.class);

  public void postDbInit() {
    // kludges for dealing with HSQLDB
    SqlRowSet rowset = jdbcTemplate.queryForRowSet("select * from INFORMATION_SCHEMA.SYSTEM_INDEXINFO where INDEX_NAME = 'OBJKEYINDEX'");

    if (!rowset.next()) {
      log.info("Creating DB index");
      jdbcTemplate.execute("CREATE INDEX objKeyIndex ON objects(bucketId, objKey)");
      jdbcTemplate.execute("CHECKPOINT DEFRAG");
    }
  }

  public Boolean insertBucket(Bucket bucket) {

    int result;

    if (bucket.bucketId == null) {
      result = jdbcTemplate.update("MERGE INTO buckets USING (VALUES(NULL,?)) AS vals(bucketId,bucketName) on buckets.bucketId = vals.bucketId WHEN NOT MATCHED THEN INSERT VALUES vals.bucketId, vals.bucketName", new java.lang.Object[]{bucket.bucketName}, new int[]{Types.VARCHAR});
    } else {
      result = jdbcTemplate.update("MERGE INTO buckets USING (VALUES(?,?)) AS vals(bucketId,bucketName) on buckets.bucketId = vals.bucketId WHEN NOT MATCHED THEN INSERT VALUES vals.bucketId, vals.bucketName", new java.lang.Object[]{bucket.bucketId, bucket.bucketName}, new int[]{Types.INTEGER, Types.VARCHAR});
    }

    if (result == 0)
      log.error("Error while creating bucket: database update failed");

    return (result > 0);
  }

}
