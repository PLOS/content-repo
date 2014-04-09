package org.plos.repo.service;

import org.plos.repo.models.Bucket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MysqlService extends SqlService {

  private static final Logger log = LoggerFactory.getLogger(HsqlService.class);

  // TODO: complain if DB does not exist, or try to create it
//  public void createDb(String dbName) {
//    jdbcTemplate.execute("CREATE DATABASE IF NOT EXISTS " + dbName);
//  }

  public void postDbInit() {

  }

  @Override
  public boolean insertBucket(Bucket bucket) {

    int result;

    Connection connection = null;
    PreparedStatement p = null;

    if (bucket.bucketId == null) {

      try {
        connection = dataSource.getConnection();
        p = connection.prepareStatement("INSERT IGNORE INTO buckets SET bucketName=?");

        p.setString(1, bucket.bucketName);

        result = p.executeUpdate();

      } catch (SQLException e) {
        log.error("error inserting bucket", e);
        return false;
      } finally {

        try {
          if (p != null)
            p.close();

          if (connection != null)
            connection.close();
        } catch (SQLException e) {
          log.error("error closing connection", e);
        }
      }

    } else {

      try {
        connection = dataSource.getConnection();
        p = connection.prepareStatement("INSERT IGNORE INTO buckets SET bucketId=?, bucketName=?");

        p.setInt(1, bucket.bucketId);
        p.setString(2, bucket.bucketName);

        result = p.executeUpdate();

      } catch (SQLException e) {
        log.error("error inserting bucket", e);
        return false;
      } finally {

        try {
          if (p != null)
            p.close();

          if (connection != null)
            connection.close();
        } catch (SQLException e) {
          log.error("error closing connection", e);
        }
      }

    }

    if (result == 0)
      log.error("Error while creating bucket: database update failed");

    return result > 0;
  }

}
