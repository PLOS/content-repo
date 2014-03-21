package org.plos.repo.service;

import org.hsqldb.types.Types;
import org.plos.repo.models.*;
import org.plos.repo.models.Object;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class HsqlService {

  private static final Logger log = LoggerFactory.getLogger(HsqlService.class);

  public static final String fileName = "plosrepo-hsqldb";

  private JdbcTemplate jdbcTemplate;

  @Required
  public void setDataSource(DriverManagerDataSource dataSource) {
    try {
      jdbcTemplate = new JdbcTemplate(dataSource);
      jdbcTemplate.execute("CHECKPOINT DEFRAG");  // kludge to deal with embedded db file growth
    } catch (Exception e2) {
      e2.printStackTrace();
    }
  }

  private static Object mapObjectRow(ResultSet rs) throws SQLException {
    return new org.plos.repo.models.Object(rs.getInt("ID"), rs.getString("KEY"), rs.getString("CHECKSUM"), rs.getTimestamp("TIMESTAMP"), rs.getString("DOWNLOADNAME"), rs.getString("CONTENTTYPE"), rs.getLong("SIZE"), rs.getString("URLS"), rs.getString("TAG"), rs.getInt("BUCKETID"), rs.getString("BUCKETNAME"), rs.getInt("VERSIONNUMBER"), Object.STATUS_VALUES.get(rs.getInt("STATUS")));
  }

  public static Bucket mapBucketRow(ResultSet rs) throws SQLException {
    return new Bucket(rs.getString("BUCKETNAME"), rs.getInt("BUCKETID"));
  }

  public Integer getBucketId(String bucketName) {
    try {
      return jdbcTemplate.queryForObject("SELECT bucketId FROM buckets WHERE bucketName=?", new java.lang.Object[]{bucketName}, Integer.class);
    } catch (EmptyResultDataAccessException e) {
      return null;
    }
  }

  public Integer deleteBucket(String bucketName) {
    return jdbcTemplate.update("DELETE FROM buckets WHERE bucketName=?", new java.lang.Object[]{bucketName}, new int[]{Types.VARCHAR});
  }

  // FOR TESTING ONLY
  public Integer deleteObject(Object object) {
    return jdbcTemplate.update("DELETE FROM objects WHERE key=? AND bucketId=? AND versionNumber=?", new java.lang.Object[]{object.key, getBucketId(object.bucketName), object.versionNumber}, new int[]{Types.VARCHAR, Types.INTEGER, Types.INTEGER});
  }

  public Integer markObjectDeleted(String key, String bucketName, int versionNumber) {
    return jdbcTemplate.update("UPDATE objects SET status=? WHERE key=? AND bucketId=? AND versionNumber=?", new java.lang.Object[]{Object.Status.DELETED.getValue(), key, getBucketId(bucketName), versionNumber}, new int[]{Types.TINYINT, Types.VARCHAR, Types.INTEGER, Types.TIMESTAMP});
  }

  public Integer getNextAvailableVersionNumber(String bucketName, String key) {

    try {
      return 1 + jdbcTemplate.queryForObject("SELECT versionNumber FROM objects a, buckets b WHERE a.bucketId = b.bucketId AND b.bucketName=? AND key=? ORDER BY versionNumber DESC LIMIT 1", new java.lang.Object[]{bucketName, key}, new int[]{Types.VARCHAR, Types.VARCHAR}, Integer.class);
    } catch (EmptyResultDataAccessException e) {
      return 0;
    }
  }

  public Object getObject(String bucketName, String key) {

    try {
      Object object = (Object) jdbcTemplate.queryForObject("SELECT * FROM objects a, buckets b WHERE a.bucketId = b.bucketId AND b.bucketName=? AND key=? AND status=? ORDER BY versionNumber DESC LIMIT 1", new java.lang.Object[]{bucketName, key, Object.Status.USED.getValue()}, new int[]{Types.VARCHAR, Types.VARCHAR, Types.TINYINT}, new RowMapper<java.lang.Object>() {
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
      Object object = (Object)jdbcTemplate.queryForObject("SELECT * FROM objects a, buckets b WHERE a.bucketId = b.bucketId AND b.bucketName=? AND key=? AND versionNumber=?", new java.lang.Object[]{bucketName, key, version}, new int[]{Types.VARCHAR, Types.VARCHAR, Types.INTEGER}, new RowMapper<java.lang.Object>() {
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

    Integer updatedEntries = jdbcTemplate.update("INSERT INTO objects (key, checksum, timestamp, bucketId, contentType, urls, downloadName, size, tag, versionNumber, status) VALUES (?, ?,?,?,?,?,?,?,?,?,?)", new java.lang.Object[]{object.key, object.checksum, object.timestamp, object.bucketId, object.contentType, object.urls, object.downloadName, object.size, object.tag, object.versionNumber, object.status.getValue()}, new int[]{Types.VARCHAR, Types.VARCHAR, Types.TIMESTAMP, Types.INTEGER, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.INTEGER, Types.VARCHAR, Types.INTEGER, Types.TINYINT});

    log.info("db object insert key " + object.key + " version " + object.versionNumber + "  " + (updatedEntries > 0 ? "SUCCESS" : "FAILURE"));

    return updatedEntries;
  }

  public Integer objectCount() throws Exception {
    return jdbcTemplate.queryForObject("SELECT COUNT(*) FROM objects WHERE status="+ Object.Status.USED.getValue(), Integer.class);
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

  public List<Bucket> listBuckets() {

    return jdbcTemplate.query("SELECT * FROM buckets", new RowMapper<Bucket>() {
      @Override
      public Bucket mapRow(ResultSet resultSet, int i) throws SQLException {
        return mapBucketRow(resultSet);
      }
    });

  }

  public List<Object> listAllObject() {

    return jdbcTemplate.query("SELECT * FROM objects a, buckets b WHERE a.bucketId = b.bucketId", new RowMapper<Object>() {
      @Override
      public Object mapRow(ResultSet resultSet, int i) throws SQLException {
        return mapObjectRow(resultSet);
      }
    });

  }

  public List<Object> listObjectsInBucket(String bucketName) {

    return jdbcTemplate.query("SELECT * FROM objects a, buckets b WHERE a.bucketId = b.bucketId AND bucketName=? AND status=?", new java.lang.Object[]{bucketName, Object.Status.USED.getValue()}, new int[]{Types.VARCHAR, Types.TINYINT}, new RowMapper<Object>() {
      @Override
      public Object mapRow(ResultSet resultSet, int i) throws SQLException {
        return mapObjectRow(resultSet);
      }
    });
  }

  public List<Object> listObjectVersions(Object object) {
    return jdbcTemplate.query("SELECT * FROM objects a, buckets b WHERE a.bucketId = b.bucketId AND bucketName=? AND key=? AND status=? ORDER BY versionNumber ASC", new java.lang.Object[]{object.bucketName, object.key, Object.Status.USED.getValue()}, new int[]{Types.VARCHAR, Types.VARCHAR, Types.TINYINT}, new RowMapper<Object>() {
      @Override
      public Object mapRow(ResultSet resultSet, int i) throws SQLException {
        return mapObjectRow(resultSet);
      }
    });
  }

}
