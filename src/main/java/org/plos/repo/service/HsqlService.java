package org.plos.repo.service;

import org.hsqldb.types.Types;
import org.plos.repo.models.Asset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class HsqlService {

  private static final Logger log = LoggerFactory.getLogger(HsqlService.class);

  public static final String fileName = "plosrepo-hsqldb";

  private JdbcTemplate jdbcTemplate;

  @Required
  public void setDataSource(DriverManagerDataSource dataSource) {
    try {
      jdbcTemplate = new JdbcTemplate(dataSource);
    } catch (Exception e2) {
      e2.printStackTrace();
    }
  }

  private static Asset mapAssetRow(ResultSet rs) throws SQLException {
    Asset asset = new Asset();

    asset.id = rs.getInt("ID"); // TODO: move field names to constants
    asset.timestamp = rs.getTimestamp("TIMESTAMP");
    asset.key = rs.getString("KEY");
    asset.checksum = rs.getString("CHECKSUM");
    asset.downloadName = rs.getString("DOWNLOADNAME");
    asset.contentType = rs.getString("CONTENTTYPE");
    asset.size = rs.getLong("SIZE");
    asset.tag = rs.getString("TAG");
    asset.url = rs.getString("URL");
    asset.bucketId = rs.getInt("BUCKETID");
    asset.bucketName = rs.getString("BUCKETNAME");

    return asset;
  }

  public Integer getBucketId(String bucketName) throws Exception {
    return jdbcTemplate.queryForObject("SELECT bucketId FROM buckets WHERE bucketName=?", new Object[]{bucketName}, Integer.class);
  }

  public Integer removeAsset(String key, String checksum, String bucketName) throws Exception {
    return jdbcTemplate.update("DELETE FROM assets WHERE key=? AND checksum=? AND bucketId=?", new Object[]{key, checksum, getBucketId(bucketName)}, new int[]{Types.VARCHAR, Types.VARCHAR, Types.INTEGER});
  }

  public Asset getAsset(String bucketName, String key)
      throws Exception {

    try {
      return (Asset) jdbcTemplate.queryForObject("SELECT * FROM assets a, buckets b WHERE a.bucketId = b.bucketId AND b.bucketName=? AND key=? ORDER BY a.timestamp DESC LIMIT 1", new Object[]{bucketName, key}, new int[]{Types.VARCHAR, Types.VARCHAR}, new RowMapper<Object>() {
        @Override
        public Asset mapRow(ResultSet resultSet, int i) throws SQLException {
          return mapAssetRow(resultSet);
        }
      });
    } catch (EmptyResultDataAccessException e) {
      return null;
    }
  }

  public Asset getAsset(String bucketName, String key, String checksum)
      throws Exception {

    try {
      return (Asset)jdbcTemplate.queryForObject("SELECT * FROM assets a, buckets b WHERE a.bucketId = b.bucketId AND b.bucketName=? AND key=? AND checksum=?", new Object[]{bucketName, key, checksum}, new int[]{Types.VARCHAR, Types.VARCHAR, Types.VARCHAR}, new RowMapper<Object>() {
        @Override
        public Asset mapRow(ResultSet resultSet, int i) throws SQLException {
          return mapAssetRow(resultSet);
        }
      });
    } catch (EmptyResultDataAccessException e) {
      return null;
    }
  }

  public Asset getAsset(String bucketName, String key, String checksum, Long fileSize) {

    try {
      return (Asset)jdbcTemplate.queryForObject("SELECT * FROM assets a, buckets b WHERE a.bucketId = b.bucketId AND b.bucketName=? AND key=? AND checksum=? AND size=?", new Object[]{bucketName, key, checksum, fileSize}, new int[]{Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.INTEGER}, new RowMapper<Object>() {
        @Override
        public Asset mapRow(ResultSet resultSet, int i) throws SQLException {
          return mapAssetRow(resultSet);
        }
      });
    } catch (EmptyResultDataAccessException e) {
      return null;
    }

  }

  public Integer insertAsset(String key, String checksum, Integer bucketId, String contentType, String downloadName, long contentSize, Date timestamp) throws SQLException {

    // TODO: make sure Asset key does not already exist

    return jdbcTemplate.update("INSERT INTO assets (key, checksum, timestamp, bucketId, contentType, downloadName, size) VALUES (?,?,?,?,?,?,?)", new Object[]{key, checksum, new Timestamp(timestamp.getTime()), bucketId, contentType, downloadName, contentSize}, new int[]{Types.VARCHAR, Types.VARCHAR, Types.TIMESTAMP, Types.INTEGER, Types.VARCHAR, Types.VARCHAR, Types.INTEGER});

  }

  public Integer assetCount() throws Exception {
    return jdbcTemplate.queryForObject("SELECT COUNT(*) FROM assets", Integer.class);
  }

  public Boolean insertBucket(String name, Integer id) throws Exception {

    int result;

    if (id == null) {
      result = jdbcTemplate.update("MERGE INTO buckets USING (VALUES(NULL,?)) AS vals(bucketId,bucketName) on buckets.bucketId = vals.bucketId WHEN NOT MATCHED THEN INSERT VALUES vals.bucketId, vals.bucketName", new Object[]{name}, new int[]{Types.VARCHAR});
    } else {
      result = jdbcTemplate.update("MERGE INTO buckets USING (VALUES(?,?)) AS vals(bucketId,bucketName) on buckets.bucketId = vals.bucketId WHEN NOT MATCHED THEN INSERT VALUES vals.bucketId, vals.bucketName", new Object[]{id, name}, new int[]{Types.INTEGER, Types.VARCHAR});
    }

    if (result == 0)
      log.error("Error while creating bucket: database update failed");

    return (result > 0);
  }

  public List<Map<String, Object>> listBuckets() throws Exception {
    // TODO: put this in a Bucket class
    return  jdbcTemplate.queryForList("SELECT * FROM assets a, buckets b WHERE a.bucketId = b.bucketId");
  }

  public List<Asset> listAssets() throws Exception {

    return jdbcTemplate.query("SELECT * FROM assets a, buckets b WHERE a.bucketId = b.bucketId", new RowMapper<Asset>() {
      @Override
      public Asset mapRow(ResultSet resultSet, int i) throws SQLException {
        return mapAssetRow(resultSet);
      }
    });

  }

}
