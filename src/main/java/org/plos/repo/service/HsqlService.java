package org.plos.repo.service;

import org.plos.repo.models.Bucket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class HsqlService extends SqlService {

  private static final Logger log = LoggerFactory.getLogger(HsqlService.class);

  public void postDbInit() {

    // kludges for dealing with HSQLDB

    Connection connection = null;
    PreparedStatement p = null;

    try {
      connection = dataSource.getConnection();
      p = connection.prepareStatement("select * from INFORMATION_SCHEMA.SYSTEM_INDEXINFO where INDEX_NAME = 'OBJKEYINDEX'");

      ResultSet result = p.executeQuery();

      PreparedStatement pc = null;

      if (result.next()) {

        try {

          pc = connection.prepareStatement("CREATE INDEX objKeyIndex ON objects(bucketId, objKey)");

          pc.execute();

        } catch (SQLException e) {
          // TODO: handle the error
        } finally {

          try {
            if (pc != null)
              pc.close();

            //connection.close();
          } catch (SQLException e) {

            // TODO: handle exception
          }
        }


      } else {
        log.info("Creating DB index");

        try {

          pc = connection.prepareStatement("CHECKPOINT DEFRAG");
          pc.execute();

        } catch (SQLException e) {
          // TODO: handle the error
        } finally {

          try {
            if (pc != null)
              pc.close();

            //connection.close();
          } catch (SQLException e) {

            // TODO: handle exception
          }
        }
      }

    } catch (SQLException e) {
      // TODO: handle the error
    } finally {

      try {
        if (p != null)
          p.close();

        if (connection != null)
          connection.close();
      } catch (SQLException e) {

        // TODO: handle exception
      }
    }

  }

  public boolean insertBucket(Bucket bucket) {

    int result;

    Connection connection = null;
    PreparedStatement p = null;

    if (bucket.bucketId == null) {

      try {

        connection = dataSource.getConnection();
        p = connection.prepareStatement("MERGE INTO buckets USING (VALUES(NULL,?)) AS vals(bucketId,bucketName) on buckets.bucketId = vals.bucketId WHEN NOT MATCHED THEN INSERT VALUES vals.bucketId, vals.bucketName");

        p.setString(1, bucket.bucketName);

        result = p.executeUpdate();

      } catch (SQLException e) {
        // TODO: handle the error
        return false;
      } finally {

        try {
          if (p != null)
            p.close();

          if (connection != null)
            connection.close();
        } catch (SQLException e) {

          // TODO: handle exception
        }
      }

    } else {

      try {

        connection = dataSource.getConnection();
        p = connection.prepareStatement("MERGE INTO buckets USING (VALUES(?,?)) AS vals(bucketId,bucketName) on buckets.bucketId = vals.bucketId WHEN NOT MATCHED THEN INSERT VALUES vals.bucketId, vals.bucketName");

        p.setInt(1, bucket.bucketId);
        p.setString(2, bucket.bucketName);

        result = p.executeUpdate();

      } catch (SQLException e) {
        // TODO: handle the error
        return false;
      } finally {

        try {
          if (p != null)
            p.close();

          if (connection != null)
            connection.close();
        } catch (SQLException e) {

          // TODO: handle exception
        }
      }

    }

    if (result == 0)
      log.error("Error while creating bucket: database update failed");

    return (result > 0);
  }

}
