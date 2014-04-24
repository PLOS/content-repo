package org.plos.repo.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PreDestroy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class HsqlService extends SqlService {

  private static final Logger log = LoggerFactory.getLogger(HsqlService.class);

  @PreDestroy
  public void destroy() throws Exception {

    // kludge for dealing with HSQLDB pooling and unit tests

    PreparedStatement p = null;
    Connection connection = null;

    connection = dataSource.getConnection();
    p = connection.prepareStatement("CHECKPOINT");
    p.execute();

    if (p != null)
      p.close();

    if (connection != null)
      connection.close();

  }

  public void postDbInit() throws Exception {

    // kludges for dealing with HSQLDB

    Connection connection = null;
    PreparedStatement p = null;

    connection = dataSource.getConnection();
    p = connection.prepareStatement("select * from INFORMATION_SCHEMA.SYSTEM_INDEXINFO where INDEX_NAME = 'OBJKEYINDEX'");

    ResultSet result = p.executeQuery();

    PreparedStatement pc = null;

    if (result.next()) {
      pc = connection.prepareStatement("CREATE INDEX objKeyIndex ON objects(bucketId, objKey)");

    } else {
      log.info("Creating DB index");
      pc = connection.prepareStatement("CHECKPOINT DEFRAG");
    }

    pc.execute();

    if (p != null)
      p.close();

    if (pc != null)
      pc.close();

    if (connection != null)
      connection.close();

   }
/*
  public boolean insertBucket(Bucket bucket) {

       int result;

       Connection connection = null;
       PreparedStatement p = null;

//       if (bucket.bucketId == null) {

         try {

           connection = dataSource.getConnection();

           p = connection.prepareStatement("MERGE INTO buckets USING (VALUES(NULL,?)) AS vals(bucketId,bucketName) on buckets.bucketId = vals.bucketId WHEN NOT MATCHED THEN INSERT VALUES vals.bucketId, vals.bucketName");

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
//
//       } else {
//
//         try {
//
//           connection = dataSource.getConnection();
//
//           p = connection.prepareStatement("MERGE INTO buckets USING (VALUES   (?,?)) AS vals(bucketId,bucketName) on buckets.bucketId = vals.bucketId WHEN NOT MATCHED THEN INSERT VALUES vals.bucketId, vals.bucketName");
//
//           p.setInt(1, bucket.bucketId);
//           p.setString(2, bucket.bucketName);
//
//           result = p.executeUpdate();
//
//         } catch (SQLException e   ) {
//           log.error("error inserting bucket", e);
//           return false;
//         } finally {
//
//           try {
//             if (p != null)
//               p.close();
//
//             if (connection != null)
//               connection.close();
//
//           } catch (SQLException e) {
//             log.error("error closing connection", e);
//           }
//         }
//
//       }

       if (result == 0)
         log.error("Error while creating bucket: database update failed");

       return (result > 0);
     }
*/
   }
