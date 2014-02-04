package org.plos.repo.service;

import org.apache.commons.io.FileUtils;
import org.hsqldb.types.Types;
import org.plos.repo.models.Asset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import java.io.File;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class HsqlService {

  private static final Logger log = LoggerFactory.getLogger(HsqlService.class);

  private static final String fileName = "plosrepo-hsqldb";

  private JdbcTemplate jdbcTemplate;

  private String directory = "";


  private void execSQLScript(String sqlScriptResource) throws Exception {

    String sqlText = FileUtils.readFileToString(new File(getClass().getResource(sqlScriptResource).toURI()));

    String sqlStatements[] = sqlText.replaceAll("--.*[\n\r]", "").split(";");

    for (String sqlStatement : sqlStatements) {
      if (sqlStatement.trim().length() > 0) {
        log.info(sqlStatement);
        jdbcTemplate.execute(sqlStatement);
      }
    }

  }

  @Required
  public void setPreferences(Preferences preferences) {
    directory = preferences.getDataDirectory();
    connectToDb();
  }

  private void connectToDb() {

    try {

      DriverManagerDataSource dataSource = new DriverManagerDataSource();
      dataSource.setDriverClassName("org.hsqldb.jdbcDriver");
      dataSource.setUrl("jdbc:hsqldb:" + directory + "/" + fileName);
      dataSource.setUsername("sa");
      dataSource.setPassword("");

      jdbcTemplate = new JdbcTemplate(dataSource);

      execSQLScript("/setup.sql");  // TODO: replace with jdbc:initialize-database ?
    } catch (Exception e2) {
      e2.printStackTrace();
    }

  }

  public Integer getBucketId(String bucketName) throws Exception {
    return jdbcTemplate.queryForObject("SELECT bucketId FROM buckets WHERE bucketName=?", new Object[]{bucketName}, Integer.class);
  }

  public Integer removeAsset(String key, String checksum, String bucketName) throws Exception {

    Integer bucketId = getBucketId(bucketName);

    return jdbcTemplate.update("DELETE FROM assets WHERE key=? AND checksum=? AND bucketId=?", new Object[]{key, checksum, bucketId}, new int[]{Types.VARCHAR, Types.VARCHAR, Types.INTEGER});

//    log.info(preparedStatement.toString());

  }


  public Asset getAsset(String bucketName, String key)
      throws Exception {

    Map<String, Object> map = jdbcTemplate.queryForMap("SELECT * FROM assets a, buckets b WHERE a.bucketId = b.bucketId AND b.bucketName=? AND key=? ORDER BY a.timestamp DESC LIMIT 1", new Object[]{bucketName, key}, new int[]{Types.VARCHAR, Types.VARCHAR});

    return convertRowToAsset(map);
  }

  public Asset getAsset(String bucketName, String key, String checksum)
      throws Exception {

    Map<String, Object> map = jdbcTemplate.queryForMap("SELECT * FROM assets a, buckets b WHERE a.bucketId = b.bucketId AND b.bucketName=? AND key=? AND checksum=?", new Object[]{bucketName, key, checksum}, new int[]{Types.VARCHAR, Types.VARCHAR, Types.VARCHAR});

    return convertRowToAsset(map);
  }

  public Asset getAsset(String bucketName, String key, String checksum, Long fileSize)
      throws Exception {

    Map<String, Object> map = jdbcTemplate.queryForMap("SELECT * FROM assets a, buckets b WHERE a.bucketId = b.bucketId AND b.bucketName=? AND key=? AND checksum=? AND size=?", new Object[]{bucketName, key, checksum, fileSize}, new int[]{Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.INTEGER});

    return convertRowToAsset(map);
  }

  public Integer insertAsset(String key, String checksum, Integer bucketId, String contentType, String downloadName, long contentSize, Date timestamp) throws SQLException {

    return jdbcTemplate.update("INSERT INTO assets (key, checksum, timestamp, bucketId, contentType, downloadName, size) VALUES (?,?,?,?,?,?,?)", new Object[]{key, checksum, timestamp.getTime(), bucketId, contentType, downloadName, contentSize}, new int[]{Types.VARCHAR, Types.VARCHAR, Types.TIMESTAMP, Types.INTEGER, Types.VARCHAR, Types.VARCHAR, Types.INTEGER});

  }


  private Asset convertRowToAsset(Map<String, Object> map) throws SQLException {

    Asset asset = new Asset();

    asset.id = (Integer)map.get("ID"); // TODO: move field names to constants
    asset.timestamp = (Date)map.get("TIMESTAMP");
    asset.key = (String)map.get("KEY");
    asset.checksum = (String)map.get("CHECKSUM");
    asset.downloadName = (String)map.get("DOWNLOADNAME");
    asset.contentType = (String)map.get("CONTENTTYPE");
    asset.size = (Integer)map.get("SIZE");               // LONG vs Integer ?
    asset.tag = (String)map.get("TAG");
    asset.url = (String)map.get("URL");
    asset.bucketId = (Integer)map.get("BUCKETID");
    asset.bucketName = (String)map.get("BUCKETNAME");

    log.info("returning asset " + asset.id);

    return asset;
  }

  public Integer assetCount() throws Exception {
    return jdbcTemplate.queryForObject("SELECT COUNT(*) FROM assets", new Object[]{}, Integer.class);
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

    ArrayList<Asset> assets = new ArrayList<>();

    List<Map<String, Object>> rows = jdbcTemplate.queryForList("SELECT * FROM assets a, buckets b WHERE a.bucketId = b.bucketId");

    for (Map<String, Object> row : rows) {
      assets.add(convertRowToAsset(row));
    }

    return assets;
  }

}
