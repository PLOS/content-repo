package org.plos.repo.service;

import org.plos.repo.models.Bucket;
import org.plos.repo.models.Object;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public abstract class SqlService {

  private static final Logger log = LoggerFactory.getLogger(SqlService.class);

  protected DataSource dataSource;
//  protected Connection connection;

//  @Required
//  public void setDataSource(DataSource dataSource) {
//    try {
//      jdbcTemplate = new JdbcTemplate(dataSource);
//    } catch (Exception e) {
//      log.error("Error setting up jdbc", e);
//    }
//
//    postDbInit();
//  }

  @Required
  public void setDataSource(DataSource dataSource) {
    try {
      this.dataSource = dataSource;
//      this.connection = dataSource.getConnection();
    } catch (Exception e) {
      log.error("Error setting up jdbc", e);
    }

    postDbInit();
  }

//  @Required
//  public void setConnection(Connection connection) {
//    this.connection = connection;
//    postDbInit();
//  }

  public abstract void postDbInit();

  private static org.plos.repo.models.Object mapObjectRow(ResultSet rs) throws SQLException {
    return new org.plos.repo.models.Object(rs.getInt("ID"), rs.getString("OBJKEY"), rs.getString("CHECKSUM"), rs.getTimestamp("TIMESTAMP"), rs.getString("DOWNLOADNAME"), rs.getString("CONTENTTYPE"), rs.getLong("SIZE"), rs.getString("URLS"), rs.getString("TAG"), rs.getInt("BUCKETID"), rs.getString("BUCKETNAME"), rs.getInt("VERSIONNUMBER"), Object.STATUS_VALUES.get(rs.getInt("STATUS")));
  }

  public static Bucket mapBucketRow(ResultSet rs) throws SQLException {
    return new Bucket(rs.getString("BUCKETNAME"), rs.getInt("BUCKETID"));
  }

  public Integer getBucketId(String bucketName) {

    PreparedStatement p = null;
    Connection connection = null;

    try {

      connection = dataSource.getConnection();
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

        if (connection != null)
          connection.close();
      } catch (SQLException e) {

        // TODO: handle exception
      }
    }
  }

  public int deleteBucket(String bucketName) {

    PreparedStatement p = null;
    Connection connection = null;

    try {

      connection = dataSource.getConnection();
      p = connection.prepareStatement("DELETE FROM buckets WHERE bucketName=?");

      p.setString(1, bucketName);

      return p.executeUpdate();

    } catch (SQLException e) {
      // TODO: handle the error
      return 0;
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

  // FOR TESTING ONLY
  public int deleteObject(Object object) {

    PreparedStatement p = null;
    Connection connection = null;

    try {

      connection = dataSource.getConnection();

      p = connection.prepareStatement("DELETE FROM objects WHERE objKey=? AND bucketId=? AND versionNumber=?");

      p.setString(1, object.key);
      p.setInt(2, object.bucketId);
      p.setInt(3, object.versionNumber);

      return p.executeUpdate();

    } catch (SQLException e) {
      // TODO: handle the error
      return 0;
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

  public int markObjectDeleted(String key, String bucketName, int versionNumber) {

    PreparedStatement p = null;
    Connection connection = null;

    try {

      connection = dataSource.getConnection();

      p = connection.prepareStatement("UPDATE objects SET status=? WHERE objKey=? AND bucketId=? AND versionNumber=?");

      p.setInt(1, Object.Status.DELETED.getValue());
      p.setString(2, key);
      p.setInt(3, getBucketId(bucketName));
      p.setInt(4, versionNumber);

      return p.executeUpdate();

    } catch (SQLException e) {
      // TODO: handle the error
      return 0;
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

  public Integer getNextAvailableVersionNumber(String bucketName, String key) {

    PreparedStatement p = null;
    Connection connection = null;

    try {

      connection = dataSource.getConnection();

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

        if (connection != null)
          connection.close();
      } catch (SQLException e) {

        // TODO: handle exception
      }
    }

  }

  public Object getObject(String bucketName, String key) {

    PreparedStatement p = null;
    Connection connection = null;

    try {
      connection = dataSource.getConnection();

      p = connection.prepareStatement("SELECT * FROM objects a, buckets b WHERE a.bucketId = b.bucketId AND b.bucketName=? AND objKey=? AND status=? ORDER BY versionNumber DESC LIMIT 1");

      p.setString(1, bucketName);
      p.setString(2, key);
      p.setInt(3, Object.Status.USED.getValue());

      ResultSet result = p.executeQuery();

      if (result.next()) {
        Object object = mapObjectRow(result);

        if (object.status == Object.Status.DELETED) {
          log.info("searched for object which has been deleted. id: " + object.id);
          return null;
        }

        return object;
      }
      else
        return null;

    } catch (SQLException e) {
      // TODO: handle the error
      return null;
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

  public Object getObject(String bucketName, String key, Integer version) {

    PreparedStatement p = null;
    Connection connection = null;

    try {
      connection = dataSource.getConnection();

      p = connection.prepareStatement("SELECT * FROM objects a, buckets b WHERE a.bucketId = b.bucketId AND b.bucketName=? AND objKey=? AND versionNumber=?");

      p.setString(1, bucketName);
      p.setString(2, key);
      p.setInt(3, version);

      ResultSet result = p.executeQuery();

      if (result.next()) {
        Object object = mapObjectRow(result);

        if (object.status == Object.Status.DELETED) {
          log.info("searched for object which has been deleted. id: " + object.id);
          return null;
        }

        return object;
      }
      else
        return null;

    } catch (SQLException e) {
      // TODO: handle the error
      return null;
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

  public int insertObject(Object object) {

    PreparedStatement p = null;
    Connection connection = null;

    try {
      connection = dataSource.getConnection();

      p = connection.prepareStatement("INSERT INTO objects (objKey, checksum, timestamp, bucketId, contentType, urls, downloadName, size, tag, versionNumber, status) VALUES (?,?,?,?,?,?,?,?,?,?,?)");

      p.setString(1, object.key);
      p.setString(2, object.checksum);
      p.setTimestamp(3, object.timestamp);
      p.setInt(4, object.bucketId);
      p.setString(5, object.contentType);
      p.setString(6, object.urls);
      p.setString(7, object.downloadName);
      p.setLong(8, object.size);
      p.setString(9, object.tag);
      p.setInt(10, object.versionNumber);
      p.setInt(11, object.status.getValue());

      return p.executeUpdate();

    } catch (SQLException e) {
      log.error("error while inserting object", e);
      return 0;
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

  public Integer objectCount() throws Exception {

    PreparedStatement p = null;
    Connection connection = null;

    try {
      connection = dataSource.getConnection();

      p = connection.prepareStatement("SELECT COUNT(*) FROM objects WHERE status=?");

      p.setInt(1, Object.Status.USED.getValue());
      ResultSet result = p.executeQuery();

      if (result.next())
        return result.getInt(1);
      else
        return null;

    } catch (SQLException e) {
      log.error("Error finding object count", e);

      // TODO: handle the error
      return null;
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

  public abstract boolean insertBucket(Bucket bucket);


  public List<Bucket> listBuckets() {

    List<Bucket> buckets = new ArrayList<>();

    PreparedStatement p = null;
    Connection connection = null;

    try {
      connection = dataSource.getConnection();

      p = connection.prepareStatement("SELECT * FROM buckets");

      ResultSet result = p.executeQuery();

      while (result.next()) {
        Bucket bucket = mapBucketRow(result);
        buckets.add(bucket);
      }

      return buckets;

    } catch (SQLException e) {
      log.error("error listing buckets", e);
      return null;
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

  public List<Object> listAllObject() {

    List<Object> objects = new ArrayList<>();

    PreparedStatement p = null;
    Connection connection = null;

    try {
      connection = dataSource.getConnection();

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

        if (connection != null)
          connection.close();
      } catch (SQLException e) {

        // TODO: handle exception
      }
    }

  }

  public List<Object> listObjectsInBucket(String bucketName) {

    List<Object> objects = new ArrayList<>();

    PreparedStatement p = null;
    Connection connection = null;

    try {
      connection = dataSource.getConnection();

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

        if (connection != null)
          connection.close();
      } catch (SQLException e) {

        // TODO: handle exception
      }
    }

  }

  public List<Object> listObjectVersions(Object object) {

    List<Object> objects = new ArrayList<>();

    PreparedStatement p = null;
    Connection connection = null;

    try {
      connection = dataSource.getConnection();

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

        if (connection != null)
          connection.close();
      } catch (SQLException e) {

        // TODO: handle exception
      }
    }

  }

}
