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
    return new Asset(rs.getInt("ID"), rs.getString("KEY"), rs.getString("CHECKSUM"), rs.getTimestamp("TIMESTAMP"), rs.getString("DOWNLOADNAME"), rs.getString("CONTENTTYPE"), rs.getLong("SIZE"), rs.getString("URLS"), rs.getString("TAG"), rs.getInt("BUCKETID"), rs.getString("BUCKETNAME"), rs.getInt("VERSIONNUMBER"), Asset.STATUS_VALUES.get(rs.getInt("STATUS")));
  }

  public static Bucket mapBucketRow(ResultSet rs) throws SQLException {
    return new Bucket(rs.getString("BUCKETNAME"), rs.getInt("BUCKETID"));
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

  // FOR TESTING ONLY
  public Integer deleteAsset(Asset asset) {
    return jdbcTemplate.update("DELETE FROM assets WHERE key=? AND bucketId=? AND versionNumber=?", new Object[]{asset.key, getBucketId(asset.bucketName), asset.versionNumber}, new int[]{Types.VARCHAR, Types.INTEGER, Types.INTEGER});
  }

  public Integer markAssetDeleted(String key, String bucketName, int versionNumber) {
    return jdbcTemplate.update("UPDATE assets SET status=? WHERE key=? AND bucketId=? AND versionNumber=?", new Object[]{Asset.Status.DELETED.getValue(), key, getBucketId(bucketName), versionNumber}, new int[]{Types.TINYINT, Types.VARCHAR, Types.INTEGER, Types.TIMESTAMP});
  }

  public Asset getAsset(String bucketName, String key) {

    try {
      Asset asset = (Asset) jdbcTemplate.queryForObject("SELECT * FROM assets a, buckets b WHERE a.bucketId = b.bucketId AND b.bucketName=? AND key=? AND status=? ORDER BY versionNumber DESC LIMIT 1", new Object[]{bucketName, key, Asset.Status.USED.getValue()}, new int[]{Types.VARCHAR, Types.VARCHAR, Types.TINYINT}, new RowMapper<Object>() {
        @Override
        public Asset mapRow(ResultSet resultSet, int i) throws SQLException {
          return mapAssetRow(resultSet);
        }
      });

      if (asset.status == Asset.Status.DELETED) {
        log.info("searched for asset which has been deleted. id: " + asset.id);
        return null;
      }

      return asset;

    } catch (EmptyResultDataAccessException e) {
      return null;
    }
  }

  public Asset getAsset(String bucketName, String key, Integer version) {

    try {
      Asset asset = (Asset)jdbcTemplate.queryForObject("SELECT * FROM assets a, buckets b WHERE a.bucketId = b.bucketId AND b.bucketName=? AND key=? AND versionNumber=?", new Object[]{bucketName, key, version}, new int[]{Types.VARCHAR, Types.VARCHAR, Types.INTEGER}, new RowMapper<Object>() {
        @Override
        public Asset mapRow(ResultSet resultSet, int i) throws SQLException {
          return mapAssetRow(resultSet);
        }
      });

      if (asset.status == Asset.Status.DELETED) {
        log.info("searched for asset which has been deleted. id: " + asset.id);
        return null;
      }

      return asset;

    } catch (EmptyResultDataAccessException e) {
      //log.error("error fetching asset " + key, e);
      return null;
    }
  }

  public Integer insertAsset(Asset asset) {

    Integer updatedEntries = jdbcTemplate.update("INSERT INTO assets (key, checksum, timestamp, bucketId, contentType, urls, downloadName, size, tag, versionNumber, status) VALUES (?, ?,?,?,?,?,?,?,?,?,?)", new Object[]{asset.key, asset.checksum, asset.timestamp, asset.bucketId, asset.contentType, asset.urls, asset.downloadName, asset.size, asset.tag, asset.versionNumber, asset.status.getValue()}, new int[]{Types.VARCHAR, Types.VARCHAR, Types.TIMESTAMP, Types.INTEGER, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.INTEGER, Types.VARCHAR, Types.INTEGER, Types.TINYINT});

    log.info("db asset insert key " + asset.key + " version " + asset.versionNumber + "  " + (updatedEntries > 0 ? "SUCCESS" : "FAILURE"));

    return updatedEntries;
  }

  public Integer assetCount() throws Exception {
    return jdbcTemplate.queryForObject("SELECT COUNT(*) FROM assets WHERE status="+Asset.Status.USED.getValue(), Integer.class);
  }

  public Boolean insertBucket(Bucket bucket) {

    int result;

    if (bucket.bucketId == null) {
      result = jdbcTemplate.update("MERGE INTO buckets USING (VALUES(NULL,?)) AS vals(bucketId,bucketName) on buckets.bucketId = vals.bucketId WHEN NOT MATCHED THEN INSERT VALUES vals.bucketId, vals.bucketName", new Object[]{bucket.bucketName}, new int[]{Types.VARCHAR});
    } else {
      result = jdbcTemplate.update("MERGE INTO buckets USING (VALUES(?,?)) AS vals(bucketId,bucketName) on buckets.bucketId = vals.bucketId WHEN NOT MATCHED THEN INSERT VALUES vals.bucketId, vals.bucketName", new Object[]{bucket.bucketId, bucket.bucketName}, new int[]{Types.INTEGER, Types.VARCHAR});
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

    return jdbcTemplate.query("SELECT * FROM assets a, buckets b WHERE a.bucketId = b.bucketId AND bucketName=? AND status=?", new Object[]{bucketName, Asset.Status.USED.getValue()}, new int[]{Types.VARCHAR, Types.TINYINT}, new RowMapper<Asset>() {
      @Override
      public Asset mapRow(ResultSet resultSet, int i) throws SQLException {
        return mapAssetRow(resultSet);
      }
    });
  }

  public List<Asset> listAssetVersions(Asset asset) {
    return jdbcTemplate.query("SELECT * FROM assets a, buckets b WHERE a.bucketId = b.bucketId AND bucketName=? AND key=? AND status=? ORDER BY versionNumber ASC", new Object[]{asset.bucketName, asset.key, Asset.Status.USED.getValue()}, new int[]{Types.VARCHAR, Types.VARCHAR, Types.TINYINT}, new RowMapper<Asset>() {
      @Override
      public Asset mapRow(ResultSet resultSet, int i) throws SQLException {
        return mapAssetRow(resultSet);
      }
    });
  }

}
