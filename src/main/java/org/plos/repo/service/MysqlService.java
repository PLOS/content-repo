package org.plos.repo.service;

import org.hsqldb.types.Types;
import org.plos.repo.models.Bucket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MysqlService extends SqlService{

  private static final Logger log = LoggerFactory.getLogger(HsqlService.class);

  public void postDbInit() {

    //jdbcTemplate.execute("CREATE DATABASE IF NOT EXISTS plosrepo_unittest");
  }

  @Override
  public Boolean insertBucket(Bucket bucket) {

    int result;

    if (bucket.bucketId == null) {
      result = jdbcTemplate.update("INSERT IGNORE INTO buckets SET bucketName=?", new java.lang.Object[]{bucket.bucketName}, new int[]{Types.VARCHAR});
    } else {
      result = jdbcTemplate.update("INSERT IGNORE INTO buckets SET bucketId=?, bucketName=?", new java.lang.Object[]{bucket.bucketId, bucket.bucketName}, new int[]{Types.INTEGER, Types.VARCHAR});
    }

    if (result == 0)
      log.error("Error while creating bucket: database update failed");

    return (result > 0);
  }

}
