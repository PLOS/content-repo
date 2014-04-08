package org.plos.repo.service;

import org.hsqldb.types.Types;
import org.plos.repo.models.Bucket;
import org.plos.repo.models.Object;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.dao.EmptyResultDataAccessException;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public abstract class SqlService {

  private static final Logger log = LoggerFactory.getLogger(SqlService.class);

//  protected JdbcTemplate jdbcTemplate;

  protected Connection connection;

  @Required
  public void setDataSource(DataSource dataSource) {
    try {
      jdbcTemplate = new JdbcTemplate(dataSource);
    } catch (Exception e) {
      log.error("Error setting up jdbc", e);
    }

    postDbInit();

  }

  public abstract void postDbInit();

  private static org.plos.repo.models.Object mapObjectRow(ResultSet rs) throws SQLException {
    return new org.plos.repo.models.Object(rs.getInt("ID"), rs.getString("OBJKEY"), rs.getString("CHECKSUM"), rs.getTimestamp("TIMESTAMP"), rs.getString("DOWNLOADNAME"), rs.getString("CONTENTTYPE"), rs.getLong("SIZE"), rs.getString("URLS"), rs.getString("TAG"), rs.getInt("BUCKETID"), rs.getString("BUCKETNAME"), rs.getInt("VERSIONNUMBER"), Object.STATUS_VALUES.get(rs.getInt("STATUS")));
  }

  public static Bucket mapBucketRow(ResultSet rs) throws SQLException {
    return new Bucket(rs.getString("BUCKETNAME"), rs.getInt("BUCKETID"));
  }

  public Integer getBucketId(String bucketName) {

    PreparedStatement p = null;

    try {

      p = connection.prepareStatement("SELECT bucketId FROM buckets WHERE bucketName=?");

      p.setString(1, bucketName);

      ResultSet result = p.executeQuery();

      if (result.next())
        return result.getInt("bucketId");
      else
        return null;

    } catch (SQLException e) {
      // TODO: handle the error
      return null;
    } finally {

      try {
        if (p != null)
          p.close();

        connection.close();
      } catch (SQLException e) {

        // TODO: handle exception
      }
    }
  }

  public boolean deleteBucket(String bucketName) {

    PreparedStatement p = null;

    try {

      p = connection.prepareStatement("DELETE FROM buckets WHERE bucketName=?");

      p.setString(1, bucketName);

      return p.execute();

    } catch (SQLException e) {
      // TODO: handle the error
      return false;
    } finally {

      try {
        if (p != null)
          p.close();

        connection.close();
      } catch (SQLException e) {

        // TODO: handle exception
      }
    }

  }

  // FOR TESTING ONLY
  public boolean deleteObject(Object object) {

    PreparedStatement p = null;

    try {

      p = connection.prepareStatement("DELETE FROM objects WHERE objKey=? AND bucketId=? AND versionNumber=?");

      p.setString(1, object.key);
      p.setInt(2, object.bucketId);
      p.setInt(3, object.versionNumber);

      return p.execute();

    } catch (SQLException e) {
      // TODO: handle the error
      return false;
    } finally {

      try {
        if (p != null)
          p.close();

        connection.close();
      } catch (SQLException e) {

        // TODO: handle exception
      }
    }

  }

  public boolean markObjectDeleted(String key, String bucketName, int versionNumber) {

    PreparedStatement p = null;

    try {

      p = connection.prepareStatement("UPDATE objects SET status=? WHERE objKey=? AND bucketId=? AND versionNumber=?");


      p.setInt(1, Object.Status.DELETED.getValue());
      p.setString(2, key);
      p.setInt(3, getBucketId(bucketName));
      p.setInt(4, versionNumber);

      return p.execute();

    } catch (SQLException e) {
      // TODO: handle the error
      return false;
    } finally {

      try {
        if (p != null)
          p.close();

        connection.close();
      } catch (SQLException e) {

        // TODO: handle exception
      }
    }

  }

  public Integer getNextAvailableVersionNumber(String bucketName, String key) {

    PreparedStatement p = null;

    try {

      p = connection.prepareStatement("SELECT versionNumber FROM objects a, buckets b WHERE a.bucketId = b.bucketId AND b.bucketName=? AND objKey=? ORDER BY versionNumber DESC LIMIT 1");

      p.setString(1, bucketName);
      p.setString(2, key);

      ResultSet result = p.executeQuery();

      if (result.next())
        return result.getInt("versionNumber");
      else
        return 0;

    } catch (SQLException e) {
      // TODO: handle the error
      return null;
    } finally {

      try {
        if (p != null)
          p.close();

        connection.close();
      } catch (SQLException e) {

        // TODO: handle exception
      }
    }

  }

  public Object getObject(String bucketName, String key) {

    try {
      Object object = (Object) jdbcTemplate.queryForObject("SELECT * FROM objects a, buckets b WHERE a.bucketId = b.bucketId AND b.bucketName=? AND objKey=? AND status=? ORDER BY versionNumber DESC LIMIT 1", new java.lang.Object[]{bucketName, key, Object.Status.USED.getValue()}, new int[]{Types.VARCHAR, Types.VARCHAR, Types.TINYINT}, new RowMapper<java.lang.Object>() {
        @Override
        public Object mapRow(ResultSet resultSet, int i) throws SQLException {
          return mapObjectRow(resultSet);
        }
      });

      if (object.status == Object.Status.DELETED) {
        log.info("searched for object which has been deleted. id: " + object.id);
        return null;
      }

      return object;

    } catch (EmptyResultDataAccessException e) {
      return null;
    }
  }

  public Object getObject(String bucketName, String key, Integer version) {

    try {
      Object object = (Object)jdbcTemplate.queryForObject("SELECT * FROM objects a, buckets b WHERE a.bucketId = b.bucketId AND b.bucketName=? AND objKey=? AND versionNumber=?", new java.lang.Object[]{bucketName, key, version}, new int[]{Types.VARCHAR, Types.VARCHAR, Types.INTEGER}, new RowMapper<java.lang.Object>() {
        @Override
        public Object mapRow(ResultSet resultSet, int i) throws SQLException {
          return mapObjectRow(resultSet);
        }
      });

      if (object.status == Object.Status.DELETED) {
        log.info("searched for object which has been deleted. id: " + object.id);
        return null;
      }

      return object;

    } catch (EmptyResultDataAccessException e) {
      //log.error("error fetching object " + key, e);
      return null;
    }
  }

  public Integer insertObject(Object object) {

    Integer updatedEntries = jdbcTemplate.update("INSERT INTO objects (objKey, checksum, timestamp, bucketId, contentType, urls, downloadName, size, tag, versionNumber, status) VALUES (?, ?,?,?,?,?,?,?,?,?,?)", new java.lang.Object[]{object.key, object.checksum, object.timestamp, object.bucketId, object.contentType, object.urls, object.downloadName, object.size, object.tag, object.versionNumber, object.status.getValue()}, new int[]{Types.VARCHAR, Types.VARCHAR, Types.TIMESTAMP, Types.INTEGER, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.INTEGER, Types.VARCHAR, Types.INTEGER, Types.TINYINT});

    log.info("db object insert key " + object.key + " version " + object.versionNumber + "  " + (updatedEntries > 0 ? "SUCCESS" : "FAILURE"));

    return updatedEntries;
  }

  public Integer objectCount() throws Exception {

    List<Bucket> buckets = new ArrayList<>();

    PreparedStatement p = null;

    try {

      p = connection.prepareStatement("SELECT COUNT(*) FROM objects WHERE status=?");

      p.setInt(1, Object.Status.USED.getValue());
      ResultSet result = p.executeQuery();

      if (result.next())
        return result.getInt(0);
      else
        return null;

    } catch (SQLException e) {
      // TODO: handle the error
      return null;
    } finally {

      try {
        if (p != null)
          p.close();

        connection.close();
      } catch (SQLException e) {

        // TODO: handle exception
      }
    }

  }

  public abstract Boolean insertBucket(Bucket bucket);
//  {

//    int result;
//
//    if (bucket.bucketId == null) {
//      result = jdbcTemplate.update("MERGE INTO buckets USING (VALUES(NULL,?)) AS vals(bucketId,bucketName) on buckets.bucketId = vals.bucketId WHEN NOT MATCHED THEN INSERT VALUES vals.bucketId, vals.bucketName", new java.lang.Object[]{bucket.bucketName}, new int[]{Types.VARCHAR});
//    } else {
//      result = jdbcTemplate.update("MERGE INTO buckets USING (VALUES(?,?)) AS vals(bucketId,bucketName) on buckets.bucketId = vals.bucketId WHEN NOT MATCHED THEN INSERT VALUES vals.bucketId, vals.bucketName", new java.lang.Object[]{bucket.bucketId, bucket.bucketName}, new int[]{Types.INTEGER, Types.VARCHAR});
//    }
//
//    if (result == 0)
//      log.error("Error while creating bucket: database update failed");
//
//    return (result > 0);
  //}

  public List<Bucket> listBuckets() {

    List<Bucket> buckets = new ArrayList<>();

    PreparedStatement p = null;

    try {

      p = connection.prepareStatement("SELECT * FROM buckets");

      ResultSet result = p.executeQuery();

      while (result.next()) {
        Bucket bucket = mapBucketRow(result);
        buckets.add(bucket);
      }

      return buckets;

    } catch (SQLException e) {
      // TODO: handle the error
      return null;
    } finally {

      try {
        if (p != null)
          p.close();

        connection.close();
      } catch (SQLException e) {

        // TODO: handle exception
      }
    }

  }

  public List<Object> listAllObject() {

    List<Object> objects = new ArrayList<>();

    PreparedStatement p = null;

    try {

      p = connection.prepareStatement("SELECT * FROM objects a, buckets b WHERE a.bucketId = b.bucketId");

      ResultSet result = p.executeQuery();

      while (result.next()) {
        objects.add(mapObjectRow(result));
      }

      return objects;

    } catch (SQLException e) {
      // TODO: handle the error
      return null;
    } finally {

      try {
        if (p != null)
          p.close();

        connection.close();
      } catch (SQLException e) {

        // TODO: handle exception
      }
    }

  }

  public List<Object> listObjectsInBucket(String bucketName) {

    List<Object> objects = new ArrayList<>();

    PreparedStatement p = null;

    try {

      p = connection.prepareStatement("SELECT * FROM objects a, buckets b WHERE a.bucketId = b.bucketId AND bucketName=? AND status=?");

      p.setString(1, bucketName);
      p.setInt(2, Object.Status.USED.getValue());

      ResultSet result = p.executeQuery();

      while (result.next()) {
        objects.add(mapObjectRow(result));
      }

      return objects;

    } catch (SQLException e) {
      // TODO: handle the error
      return null;
    } finally {

      try {
        if (p != null)
          p.close();

        connection.close();
      } catch (SQLException e) {

        // TODO: handle exception
      }
    }

  }

  public List<Object> listObjectVersions(Object object) {

    List<Object> objects = new ArrayList<>();

    PreparedStatement p = null;

    try {

      p = connection.prepareStatement("SELECT * FROM objects a, buckets b WHERE a.bucketId = b.bucketId AND bucketName=? AND objKey=? AND status=? ORDER BY versionNumber ASC");

      p.setString(1, object.bucketName);
      p.setString(2, object.key);
      p.setInt(3, Object.Status.USED.getValue());

      ResultSet result = p.executeQuery();

      while (result.next()) {
        objects.add(mapObjectRow(result));
      }

      return objects;

    } catch (SQLException e) {
      // TODO: handle the error
      return null;
    } finally {

      try {
        if (p != null)
          p.close();

        connection.close();
      } catch (SQLException e) {

        // TODO: handle exception
      }
    }

  }

}
