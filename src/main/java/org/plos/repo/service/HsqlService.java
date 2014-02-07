package org.plos.repo.service;

import org.hsqldb.types.Types;
import org.plos.repo.models.Asset;
import org.plos.repo.models.Bucket;
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
    } catch (Exception e2) {
      e2.printStackTrace();
    }
  }

  private static Asset mapAssetRow(ResultSet rs) throws SQLException {
    return new Asset(rs.getInt("ID"), rs.getString("KEY"), rs.getString("CHECKSUM"), rs.getTimestamp("TIMESTAMP"), rs.getString("DOWNLOADNAME"), rs.getString("CONTENTTYPE"), rs.getLong("SIZE"), rs.getString("URL"), rs.getString("TAG"), rs.getInt("BUCKETID"), rs.getString("BUCKETNAME"), rs.getInt("VERSIONNUMBER"));
  }

  public static Bucket mapBucketRow(ResultSet rs) throws SQLException {
    Bucket bucket = new Bucket();

    bucket.bucketId = rs.getInt("BUCKETID");  // TODO: move string to const
    bucket.bucketName = rs.getString("BUCKETNAME");

    return bucket;
  }

  public Integer getBucketId(String bucketName) {
    try {
      return jdbcTemplate.queryForObject("SELECT bucketId FROM buckets WHERE bucketName=?", new Object[]{bucketName}, Integer.class);
    } catch (EmptyResultDataAccessException e) {
      return null;
    }
  }

  public Integer deleteBucket(String bucketName) {
    return jdbcTemplate.update("DELETE FROM buckets WHERE bucketName=?", new Object[]{bucketName}, new int[]{Types.VARCHAR});
  }

//  public Integer deleteAsset(String key, String checksum, String bucketName, Timestamp timestamp) {
//    return jdbcTemplate.update("DELETE FROM assets WHERE key=? AND checksum=? AND bucketId=? AND timestamp=?", new Object[]{key, checksum, getBucketId(bucketName), timestamp}, new int[]{Types.VARCHAR, Types.VARCHAR, Types.INTEGER, Types.TIMESTAMP});
//  }


  // FOR TESTING ONLY
  public Integer deleteAsset(String key, String checksum, String bucketName, int versionNumber) {
    return jdbcTemplate.update("DELETE FROM assets WHERE key=? AND checksum=? AND bucketId=? AND versionNumber=?", new Object[]{key, checksum, getBucketId(bucketName), versionNumber}, new int[]{Types.VARCHAR, Types.VARCHAR, Types.INTEGER, Types.INTEGER});
  }

  public Integer markAssetDeleted(String key, String checksum, String bucketName, int versionNumber) {
    return jdbcTemplate.update("UPDATE assets SET tag='deleted' WHERE key=? AND checksum=? AND bucketId=? AND versionNumber=?", new Object[]{key, checksum, getBucketId(bucketName), versionNumber}, new int[]{Types.VARCHAR, Types.VARCHAR, Types.INTEGER, Types.TIMESTAMP});
  }

  public boolean assetInUse(String bucketName, String checksum) {
    return (jdbcTemplate.queryForList("SELECT * FROM assets a, buckets b WHERE a.bucketId = b.bucketId AND b.bucketName=? AND checksum=? ORDER BY a.timestamp DESC LIMIT 1", new Object[]{bucketName, checksum}, new int[]{Types.VARCHAR, Types.VARCHAR}).size() > 0);
  }

  public Asset getAsset(String bucketName, String key) {

    try {
      return (Asset) jdbcTemplate.queryForObject("SELECT * FROM assets a, buckets b WHERE a.bucketId = b.bucketId AND b.bucketName=? AND key=? AND (tag IS NULL OR NOT tag=?) ORDER BY a.timestamp DESC LIMIT 1", new Object[]{bucketName, key, "deleted"}, new int[]{Types.VARCHAR, Types.VARCHAR, Types.VARCHAR}, new RowMapper<Object>() {
        @Override
        public Asset mapRow(ResultSet resultSet, int i) throws SQLException {
          return mapAssetRow(resultSet);
        }
      });
    } catch (EmptyResultDataAccessException e) {
      return null;
    }
  }

  public Asset getAsset(String bucketName, String key, String checksum) {

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

  public Integer insertAsset(Asset asset) {
    return jdbcTemplate.update("INSERT INTO assets (key, checksum, timestamp, bucketId, contentType, downloadName, size, tag, versionNumber) VALUES (?,?,?,?,?,?,?,?,?)", new Object[]{asset.key, asset.checksum, asset.timestamp, asset.bucketId, asset.contentType, asset.downloadName, asset.size, asset.tag, asset.versionNumber}, new int[]{Types.VARCHAR, Types.VARCHAR, Types.TIMESTAMP, Types.INTEGER, Types.VARCHAR, Types.VARCHAR, Types.INTEGER, Types.VARCHAR, Types.INTEGER});
  }

  public Integer assetCount() throws Exception {
    return jdbcTemplate.queryForObject("SELECT COUNT(*) FROM assets", Integer.class);
  }

  public Boolean insertBucket(String name, Integer id) {

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

  public List<Bucket> listBuckets() {

    return jdbcTemplate.query("SELECT * FROM buckets", new RowMapper<Bucket>() {
      @Override
      public Bucket mapRow(ResultSet resultSet, int i) throws SQLException {
        return mapBucketRow(resultSet);
      }
    });

  }

  public List<Asset> listAllAssets() {

    return jdbcTemplate.query("SELECT * FROM assets a, buckets b WHERE a.bucketId = b.bucketId", new RowMapper<Asset>() {
      @Override
      public Asset mapRow(ResultSet resultSet, int i) throws SQLException {
        return mapAssetRow(resultSet);
      }
    });

  }

  public List<Asset> listAssetsInBucket(String bucketName) {

    return jdbcTemplate.query("SELECT * FROM assets a, buckets b WHERE a.bucketId = b.bucketId AND bucketName=?", new Object[]{bucketName}, new int[]{Types.VARCHAR}, new RowMapper<Asset>() {
      @Override
      public Asset mapRow(ResultSet resultSet, int i) throws SQLException {
        return mapAssetRow(resultSet);
      }
    });
  }

  public List<Asset> listAssetVersions(String bucketName, String key) {

    return jdbcTemplate.query("SELECT * FROM assets a, buckets b WHERE a.bucketId = b.bucketId AND bucketName=? AND key=?", new Object[]{bucketName, key}, new int[]{Types.VARCHAR, Types.VARCHAR}, new RowMapper<Asset>() {
      @Override
      public Asset mapRow(ResultSet resultSet, int i) throws SQLException {
        return mapAssetRow(resultSet);
      }
    });
  }

}
