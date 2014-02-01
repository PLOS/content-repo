package org.plos.repo.service;

import org.apache.commons.io.FileUtils;
import org.plos.repo.models.Asset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

public class HsqlService {

  private static final Logger log = LoggerFactory.getLogger(HsqlService.class);

  private static final String fileName = "plosrepo-hsqldb";

  private Connection connection = null;

  private String directory = "";


  // TODO: use pooling?

  private void execSQLScript(String sqlScriptResource) throws Exception {

    String sqlText = FileUtils.readFileToString(new File(getClass().getResource(sqlScriptResource).toURI()));

    String sqlStatements[] = sqlText.replaceAll("--.*[\n\r]", "").split(";");

    for (String sqlStatement : sqlStatements) {
      if (sqlStatement.trim().length() > 0) {
        log.info(sqlStatement);
        connection.prepareStatement(sqlStatement).execute();
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
      connection = DriverManager.getConnection("jdbc:hsqldb:" + directory + "/" + fileName);
      execSQLScript("/setup.sql");  // TODO: replace with jdbc:initialize-database ?
    } catch (Exception e2) {
      e2.printStackTrace();
    }

  }

  public Integer getBucketId(String bucketName) throws Exception {
    ResultSet rs = connection.prepareStatement("SELECT bucketId FROM buckets WHERE bucketName='"+bucketName+"'").executeQuery();

    if (!rs.next())
      return null;
    return rs.getInt(1);
  }

  public boolean assetExists(String key, String checksum, Integer bucketId, long fileSize) throws SQLException {

    PreparedStatement preparedStatement = connection.prepareStatement("SELECT * FROM assets WHERE key=? AND checksum=? AND bucketId=? AND size=?");

    preparedStatement.setString(1, key);
    preparedStatement.setString(2, checksum);
    preparedStatement.setInt(3, bucketId);
    preparedStatement.setLong(4, fileSize);

    log.info(preparedStatement.toString());

    ResultSet rs = preparedStatement.executeQuery();

    return (rs.next());
  }

  public Integer removeAsset(String key, String checksum, String bucketName) throws Exception {

    Integer bucketId = getBucketId(bucketName);

    PreparedStatement preparedStatement = connection.prepareStatement("DELETE FROM assets WHERE key=? AND checksum=? AND bucketId=?");

    preparedStatement.setString(1, key);
    preparedStatement.setString(2, checksum);
    preparedStatement.setInt(3, bucketId);

    log.info(preparedStatement.toString());

    return preparedStatement.executeUpdate();
  }


  public Asset getAsset(String bucketName, String key)
      throws Exception {

    PreparedStatement preparedStatement = connection.prepareStatement("SELECT * FROM assets a, buckets b WHERE a.bucketId = b.bucketId AND b.bucketName=? AND key=?");

    preparedStatement.setString(1, bucketName);
    preparedStatement.setString(2, key);

    ResultSet rs = preparedStatement.executeQuery();

    log.info(preparedStatement.toString());

    if (rs.next())
      return convertResultToAsset(rs);
    return null;
  }

  public Asset getAsset(String bucketName, String key, String checksum)
      throws Exception {

    PreparedStatement preparedStatement = connection.prepareStatement("SELECT * FROM assets a, buckets b WHERE a.bucketId = b.bucketId AND b.bucketName=? AND key=? AND checksum=?");

    preparedStatement.setString(1, bucketName);
    preparedStatement.setString(2, key);
    preparedStatement.setString(3, checksum);

    ResultSet rs = preparedStatement.executeQuery();

    log.info(preparedStatement.toString());

    if (rs.next())
      return convertResultToAsset(rs);
    return null;
  }

  public Asset getAsset(String bucketName, String key, String checksum, Long fileSize)
      throws Exception {

    PreparedStatement preparedStatement = connection.prepareStatement("SELECT * FROM assets a, buckets b WHERE a.bucketId = b.bucketId AND b.bucketName=? AND key=? AND checksum=? AND size=?");

    preparedStatement.setString(1, bucketName);
    preparedStatement.setString(2, key);
    preparedStatement.setString(3, checksum);
    preparedStatement.setLong(4, fileSize);

    ResultSet rs = preparedStatement.executeQuery();

    log.info(preparedStatement.toString());

    if (rs.next())
      return convertResultToAsset(rs);
    return null;
  }

  public Integer insertAsset(String key, String checksum, Integer bucketId, String contentType, String downloadName, long contentSize, Date timestamp) throws SQLException {

    PreparedStatement preparedStatement = connection.prepareStatement("INSERT INTO assets (key, checksum, timestamp, bucketId, contentType, downloadName, size) VALUES (?,?,?,?,?,?,?)");

    preparedStatement.setString(1, key);
    preparedStatement.setString(2, checksum);
    preparedStatement.setTimestamp(3, new Timestamp(timestamp.getTime()));
    preparedStatement.setInt(4, bucketId);
    preparedStatement.setString(5, contentType);
    preparedStatement.setString(6, downloadName);
    preparedStatement.setLong(7, contentSize);

    log.info(preparedStatement.toString());

    Integer result = preparedStatement.executeUpdate();
    connection.commit();

    return result;
  }

  private Asset convertResultToAsset(ResultSet rs) throws SQLException {

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

    log.info("returning asset " + asset.id);

    return asset;
  }

  private List<HashMap<String,Object>> convertResultSetToList(ResultSet rs) throws SQLException {
    ResultSetMetaData md = rs.getMetaData();
    int columns = md.getColumnCount();
    List<HashMap<String,Object>> list = new ArrayList<>();

    while (rs.next()) {
      HashMap<String,Object> row = new HashMap<>(columns);
      for(int i=1; i<=columns; ++i) {
        row.put(md.getColumnName(i),rs.getObject(i));
      }
      list.add(row);
    }

    return list;
  }

  public Integer assetCount() throws Exception {
    ResultSet rs = connection.prepareStatement("SELECT COUNT(*) FROM assets").executeQuery();
    rs.next();
    return rs.getInt(1);
  }

  public Boolean insertBucket(String name, Integer id) throws Exception {

    PreparedStatement preparedStatement;

    if (id == null) {
      preparedStatement = connection.prepareStatement("MERGE INTO buckets USING (VALUES(NULL,?)) AS vals(bucketId,bucketName) on buckets.bucketId = vals.bucketId WHEN NOT MATCHED THEN INSERT VALUES vals.bucketId, vals.bucketName");
      preparedStatement.setString(1, name);
    } else {
      preparedStatement = connection.prepareStatement("MERGE INTO buckets USING (VALUES(?,?)) AS vals(bucketId,bucketName) on buckets.bucketId = vals.bucketId WHEN NOT MATCHED THEN INSERT VALUES vals.bucketId, vals.bucketName");
      preparedStatement.setInt(1, id);
      preparedStatement.setString(2, name);
    }

    log.info(preparedStatement.toString());

    Integer result = preparedStatement.executeUpdate();
    connection.commit();

    if (result == 0)
      log.error("Error while creating bucket: database update failed");

    return (result > 0);
  }

  public List<HashMap<String, Object>> listBuckets() throws Exception {
    ResultSet rs = connection.prepareStatement("SELECT * FROM buckets").executeQuery();
    return convertResultSetToList(rs);
  }

  public List<Asset> listAssets() throws Exception {
    ResultSet rs = connection.prepareStatement("SELECT * FROM assets a, buckets b WHERE a.bucketId = b.bucketId").executeQuery();

    ArrayList<Asset> assets = new ArrayList<>();

    while (rs.next())
      assets.add(convertResultToAsset(rs));

    return assets;
  }

}
