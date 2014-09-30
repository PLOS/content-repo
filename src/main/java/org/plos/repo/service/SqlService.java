/*
 * Copyright (c) 2006-2014 by Public Library of Science
 * http://plos.org
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.plos.repo.service;

import org.plos.repo.models.Bucket;
import org.plos.repo.models.Collection;
import org.plos.repo.models.Object;
import org.plos.repo.models.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public abstract class SqlService {

  private static final Logger log = LoggerFactory.getLogger(SqlService.class);

  protected DataSource dataSource;

  private static final ThreadLocal<Connection> connectionLocal = new ThreadLocal<>();

  @Required
  public void setDataSource(DataSource dataSource) throws SQLException {
    this.dataSource = dataSource;
    postDbInit();
  }

  public abstract void postDbInit() throws SQLException;

  private static org.plos.repo.models.Object mapObjectRow(ResultSet rs) throws SQLException {
    return new org.plos.repo.models.Object(rs.getInt("ID"), rs.getString("OBJKEY"), rs.getString("CHECKSUM"),
                                          rs.getTimestamp("TIMESTAMP"), rs.getString("DOWNLOADNAME"), rs.getString("CONTENTTYPE"),
                                          rs.getLong("SIZE"), rs.getString("TAG"), rs.getInt("BUCKETID"), rs.getString("BUCKETNAME"),
                                          rs.getInt("VERSIONNUMBER"), Status.STATUS_VALUES.get(rs.getInt("STATUS")), rs.getTimestamp("CREATIONDATE"),
                                          rs.getInt("VERSIONCHECKSUM"));
  }

  private static org.plos.repo.models.Collection mapCollectionRow(ResultSet rs) throws SQLException {
    return new org.plos.repo.models.Collection(rs.getInt("ID"), rs.getString("COLLKEY"), rs.getTimestamp("TIMESTAMP"),
                                               rs.getInt("BUCKETID"), rs.getString("BUCKETNAME"), rs.getInt("VERSIONNUMBER"),
                                                Status.STATUS_VALUES.get(rs.getInt("STATUS")), rs.getString("TAG"),
                                                rs.getTimestamp("CREATIONDATE"), rs.getInt("VERSIONCHECKSUM"));
  }

  public static Bucket mapBucketRow(ResultSet rs) throws SQLException {
    return new Bucket(rs.getInt("BUCKETID"), rs.getString("BUCKETNAME"), rs.getTimestamp("TIMESTAMP"), rs.getTimestamp("CREATIONDATE"));
  }

  private static void closeDbStuff(ResultSet result, PreparedStatement p) throws SQLException {

    if (result != null)
      result.close();

    if (p != null)
      p.close();

  }

  public void getConnection() throws SQLException {
    Connection dbConnection = dataSource.getConnection();
    dbConnection.setAutoCommit(false);
    connectionLocal.set(dbConnection);
  }

  public void releaseConnection() throws SQLException {
    Connection dbConnection = connectionLocal.get();

    if (dbConnection != null) {
      dbConnection.close();
      connectionLocal.remove();
    }
  }

  public void transactionCommit() throws SQLException {
    Connection dbConnection = connectionLocal.get();
    dbConnection.commit();
  }

  public void transactionRollback() throws SQLException {
    Connection dbConnection = connectionLocal.get();
    dbConnection.rollback();
  }

  public Bucket getBucket(String bucketName) throws SQLException {

    PreparedStatement p = null;
    ResultSet result = null;

    try {

      p = connectionLocal.get().prepareStatement("SELECT * FROM buckets WHERE bucketName=?");

      p.setString(1, bucketName);

      result = p.executeQuery();

      if (result.next())
        return mapBucketRow(result);
      else
        return null;

    } finally {
      closeDbStuff(result, p);
    }
  }

  public int deleteBucket(String bucketName) throws SQLException {

    PreparedStatement p = null;

    try {

      p = connectionLocal.get().prepareStatement("DELETE FROM buckets WHERE bucketName=?");

      p.setString(1, bucketName);

      return p.executeUpdate();

    } finally {
      closeDbStuff(null, p);
    }

  }

  // FOR TESTING ONLY
  public int deleteObject(Object object) throws SQLException {

    PreparedStatement p = null;

    try {

      p = connectionLocal.get().prepareStatement("DELETE FROM objects WHERE objKey=? AND bucketId=? AND versionNumber=?");

      p.setString(1, object.key);
      p.setInt(2, object.bucketId);
      p.setInt(3, object.versionNumber);

      return p.executeUpdate();

    } finally {
      closeDbStuff(null, p);
    }

  }

  // FOR TESTING ONLY
  public int deleteCollection(Collection collection) throws SQLException {

    PreparedStatement p = null;

    try {

      p = connectionLocal.get().prepareStatement("DELETE FROM collectionObject WHERE collectionId=?");
      p.setInt(1, collection.getId());
      p.executeUpdate();

      p = connectionLocal.get().prepareStatement("DELETE FROM collections WHERE id=?");
      p.setInt(1, collection.getId());
      return p.executeUpdate();


    } finally {
      closeDbStuff(null, p);
    }

  }

  public int markObjectDeleted(String key, String bucketName, Integer version, Integer versionChecksum, String tag) throws SQLException {

    PreparedStatement p = null;

    Bucket bucket = getBucket(bucketName);

    if (bucket == null)
      return 0;

    try {

      StringBuilder query = new StringBuilder();
      query.append("UPDATE objects SET status=? WHERE objKey=? AND bucketId=?");

      if (version != null){
        query.append(" AND versionNumber=?");
      }
      if (versionChecksum != null){
        query.append(" AND versionChecksum=?");
      }
      if (tag != null){
        query.append(" AND tag=?");
      }

      p = connectionLocal.get().prepareStatement(query.toString());

      p.setInt(1, Status.DELETED.getValue());
      p.setString(2, key);
      p.setInt(3, bucket.bucketId);

      int i = 4;
      if (version != null){
        p.setInt(i++, version);
      }
      if (versionChecksum != null){
        p.setInt(i++, versionChecksum);
      }
      if (tag != null){
        p.setString(i++, tag);
      }

      return p.executeUpdate();

    } finally {
      closeDbStuff(null, p);
    }

  }

  private Integer getNextAvailableVersionNumber(String bucketName, String key, String tableName, String keyName) throws SQLException {

    PreparedStatement p = null;
    ResultSet result = null;

    try {

      StringBuilder query = new StringBuilder();
      query.append("SELECT versionNumber FROM  ");
      query.append(tableName);
      query.append(" a, buckets b WHERE a.bucketId = b.bucketId AND b.bucketName=? AND ");
      query.append(keyName);
      query.append("=? ORDER BY versionNumber DESC LIMIT 1");

      p = connectionLocal.get().prepareStatement(query.toString());

      p.setString(1, bucketName);
      p.setString(2, key);

      result = p.executeQuery();

      if (result.next())
        return result.getInt("versionNumber") + 1;
      else
        return 0;

    } finally {
      closeDbStuff(result, p);
    }

  }

  public Integer getCollectionNextAvailableVersion(String bucketName, String key) throws SQLException{
    return getNextAvailableVersionNumber(bucketName, key, "collections", "collKey");
  }

  public Integer getObjectNextAvailableVersion(String bucketName, String key) throws SQLException{
    return getNextAvailableVersionNumber(bucketName, key, "objects", "objKey");
  }

  public Object getObject(String bucketName, String key) throws SQLException {

    PreparedStatement p = null;
    ResultSet result = null;

    try {

      p = connectionLocal.get().prepareStatement("SELECT * FROM objects a, buckets b WHERE a.bucketId = b.bucketId AND b.bucketName=? AND objKey=? AND status=? ORDER BY a.creationDate DESC LIMIT 1");

      p.setString(1, bucketName);
      p.setString(2, key);
      p.setInt(3, Status.USED.getValue());

      result = p.executeQuery();

      if (result.next()) {
        Object object = mapObjectRow(result);

        if (object.status == Status.DELETED) {
          log.info("searched for object which has been deleted. id: " + object.id);
          return null;
        }

        return object;
      }
      else
        return null;

    } finally {
      closeDbStuff(result, p);
    }

  }

  public Object getObject(String bucketName, String key, Integer version, Integer versionChecksum, String tag) throws SQLException {

    PreparedStatement p = null;
    ResultSet result = null;

    try {

      StringBuilder query = new StringBuilder();
      query.append("SELECT * FROM objects a, buckets b WHERE a.bucketId = b.bucketId AND b.bucketName=? AND objKey=?");

      if (version != null){
        query.append(" AND versionNumber=?");
      }
      if (versionChecksum != null){
        query.append(" AND versionChecksum=?");
      }
      if (tag != null){
        query.append(" AND tag=?");
      }

      p = connectionLocal.get().prepareStatement(query.toString());

      p.setString(1, bucketName);
      p.setString(2, key);

      int i = 3;
      if (version != null){
        p.setInt(i++, version);
      }
      if (versionChecksum != null){
        p.setInt(i++, versionChecksum);
      }
      if (tag != null){
        p.setString(i++, tag);
      }

      result = p.executeQuery();

      if (result.next()) {
        Object object = mapObjectRow(result);

        if (object.status == Status.DELETED) {
          log.info("searched for object which has been deleted. id: " + object.id);
          return null;
        }

        return object;
      }
      else
        return null;

    } finally {
      closeDbStuff(result, p);
    }

  }

  public int insertObject(Object object) throws SQLException {

    // TODO: return object or objectid from this function?

    PreparedStatement p = null;

    try {

      p = connectionLocal.get().prepareStatement("INSERT INTO objects (objKey, checksum, timestamp, bucketId, contentType, downloadName, size, tag, versionNumber, status, creationDate, versionChecksum) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)");

      p.setString(1, object.key);
      p.setString(2, object.checksum);
      p.setTimestamp(3, object.timestamp);
      p.setInt(4, object.bucketId);
      p.setString(5, object.contentType);
      p.setString(6, object.downloadName);
      p.setLong(7, object.size);
      p.setString(8, object.tag);
      p.setInt(9, object.versionNumber);
      p.setInt(10, object.status.getValue());
      p.setTimestamp(11, object.timestamp);
      p.setInt(12, object.versionChecksum);

      return p.executeUpdate();

    } finally {
      closeDbStuff(null, p);
    }

  }

  public Integer objectCount(boolean includeDeleted, String bucketName) throws SQLException {

    PreparedStatement p = null;
    ResultSet result = null;

    try {

      StringBuilder q = new StringBuilder("SELECT COUNT(*) FROM objects a, buckets b WHERE a.bucketId = b.bucketId");
      if (!includeDeleted)
        q.append(" AND a.status=?");
      if (bucketName != null)
        q.append(" AND bucketName=?");

      p = connectionLocal.get().prepareStatement(q.toString());

      int index = 0;
      if (!includeDeleted)
        p.setInt(++index, Status.USED.getValue());
      if (bucketName != null)
        p.setString(++index, bucketName);

      result = p.executeQuery();

      if (result.next())
        return result.getInt(1);
      else
        return null;

    } finally {
      closeDbStuff(result, p);
    }
  }

  public boolean insertBucket(Bucket bucket, Timestamp creationDate) throws SQLException {

    int result;

    PreparedStatement p = null;

    try {

      p = connectionLocal.get().prepareStatement("INSERT INTO buckets (bucketName, timestamp, creationDate) VALUES(?, ?, ?)");

      p.setString(1, bucket.bucketName);
      p.setTimestamp(2, creationDate);
      p.setTimestamp(3, creationDate);

      result = p.executeUpdate();

    } finally {
      closeDbStuff(null, p);
    }

    return (result > 0);
  }

  public List<Bucket> listBuckets(Timestamp timestamp) throws SQLException {

   // TODO : add timestamp to buckets
    return null;

  }

  public List<Bucket> listBuckets() throws SQLException {

    List<Bucket> buckets = new ArrayList<>();

    PreparedStatement p = null;
    ResultSet result = null;

    try {

      p = connectionLocal.get().prepareStatement("SELECT * FROM buckets");

      result = p.executeQuery();

      while (result.next()) {
        Bucket bucket = mapBucketRow(result);
        buckets.add(bucket);
      }

      return buckets;

    } finally {
      closeDbStuff(result, p);
    }

  }

  public List<Object> listObjects(String bucketName, Integer offset, Integer limit, boolean includeDeleted, String tag) throws SQLException {

    List<Object> objects = new ArrayList<>();

    PreparedStatement p = null;
    ResultSet result = null;

    try {

      StringBuilder q = new StringBuilder();
      q.append("SELECT * FROM objects a, buckets b WHERE a.bucketId = b.bucketId");

      if (!includeDeleted)
        q.append(" AND status=?");
      if (bucketName != null)
        q.append(" AND bucketName=?");
      if (tag != null)
        q.append(" AND TAG=?");
      if (limit != null)
        q.append(" LIMIT " + limit);
      if (offset != null)
        q.append(" OFFSET " + offset);
      p = connectionLocal.get().prepareStatement(q.toString());

      int i = 1;

      if (!includeDeleted)
        p.setInt(i++, Status.USED.getValue());

      if (bucketName != null)
        p.setString(i++, bucketName);

      if (tag != null)
        p.setString(i++, tag);

      result = p.executeQuery();

      while (result.next()) {
        objects.add(mapObjectRow(result));
      }

      return objects;

    } finally {
      closeDbStuff(result, p);
    }

  }

  public List<Object> listObjects(Timestamp timestamp) throws SQLException {

    List<Object> objects = new ArrayList<>();

    PreparedStatement p = null;
    ResultSet result = null;

    try {

      StringBuilder q = new StringBuilder();
      q.append("SELECT * FROM objects WHERE timestamp >= ?");

      p.setTimestamp(1, timestamp);

      result = p.executeQuery();

      while (result.next()) {
        objects.add(mapObjectRow(result));
      }

      return objects;

    } finally {
      closeDbStuff(result, p);
    }

  }

  /**
   * Returns a list of collections for the given bucket name <code>bucketName</code>
   * @param bucketName a single String representing the bucket name where the collection is stored
   * @param offset an Integer used to determine the offset of the response
   * @param limit an Integer used to determine the limit of the response
   * @param includeDeleted a Boolean used to define is the response will include delete collections or not
   * @param tag a single String used to filter the collections regarding the tag property
   * @return a list of {@link Collection}
   * @throws SQLException
   */
  public List<Collection> listCollections(String bucketName, Integer offset, Integer limit, Boolean includeDeleted, String tag) throws SQLException {

    List<Collection> collections = new ArrayList<>();

    PreparedStatement p = null;
    ResultSet result = null;

    try {

      StringBuilder q = new StringBuilder();
      q.append("SELECT * FROM collections c, buckets b WHERE c.bucketId = b.bucketId");

      if (!includeDeleted)
        q.append(" AND status=?");
      if (bucketName != null)
        q.append(" AND bucketName=?");
      if (tag != null)
        q.append(" AND TAG=?");
      if (limit != null)
        q.append(" LIMIT " + limit);
      if (offset != null)
        q.append(" OFFSET " + offset);

      p = connectionLocal.get().prepareStatement(q.toString());

      int i = 1;
      if (!includeDeleted)
        p.setInt(i++, Status.USED.getValue());

      if (bucketName != null)
        p.setString(i++, bucketName);

      if (tag != null)
        p.setString(i++, tag);

      result = p.executeQuery();

      while (result.next()) {
        Collection c = mapCollectionRow(result);
        c.addObjects(listCollectionObjects(c.getId()));
        collections.add(c);
      }

      return collections;

    } finally {
      closeDbStuff(result, p);
    }

  }

  public List<Collection> listCollections(Timestamp timestamp) throws SQLException {

    List<Collection> collections = new ArrayList<>();

    PreparedStatement p = null;
    ResultSet result = null;

    try {

      StringBuilder q = new StringBuilder();
      q.append("SELECT * FROM collections WHERE c.timestamp > ?");

      p = connectionLocal.get().prepareStatement(q.toString());

      p.setTimestamp(1, timestamp);

      while (result.next()) {
        collections.add(mapCollectionRow(result));
      }

      return collections;

    } finally {
      closeDbStuff(result, p);
    }

  }

  /**
   * Returns the list of objects contains in the given collection <code>id</code>
   * @param id an integer representing the collection id
   * @return a list of {@link Object }
   * @throws SQLException
   */
  protected List<Object> listCollectionObjects(Integer id) throws SQLException {

    List<Object> objects = new ArrayList<>();

    PreparedStatement p = null;
    ResultSet result = null;

    try {

      StringBuilder q = new StringBuilder();
      q.append(" SELECT o.*, b.*\n" +
          "FROM objects o, collectionObject co, buckets b\n" +
          "WHERE co.collectionId = ?\n" +
          "AND co.objectId = o.id\n" +
          "AND o.bucketId = b.bucketId");

      p = connectionLocal.get().prepareStatement(q.toString());

      p.setInt(1, id);

      result = p.executeQuery();

      while (result.next()) {
        objects.add(mapObjectRow(result));
      }

      return objects;

    } finally {
      closeDbStuff(result, p);
    }

  }

  /**
   * Fetch the latest version in used of the collection defined by <code>bucketName</code> & <code>key</code>. The latest
   * version is defined as the latest created collection with status = USED.
   * @param bucketName a single String representing the bucket name where the collection is stored
   * @param key a single String identifying the collection key
   * @return {@link org.plos.repo.models.Collection}
   * @throws SQLException
   */
  public Collection getCollection(String bucketName, String key) throws SQLException {

    PreparedStatement p = null;
    ResultSet result = null;

    try {

      StringBuilder query = new StringBuilder();
      query.append("SELECT * FROM collections a, buckets b WHERE a.bucketId = b.bucketId AND b.bucketName=? " +
          "AND collKey=? AND status=? ORDER BY a.creationDate DESC LIMIT 1");

      p = connectionLocal.get().prepareStatement(query.toString());

      p.setString(1, bucketName);
      p.setString(2, key);
      p.setInt(3, Status.USED.getValue());

      result = p.executeQuery();

      if (result.next()) {
        Collection collection = mapCollectionRow(result);

        if (collection.getStatus() == Status.DELETED) {
          log.info("searched for collection which has been deleted. id: " + collection.getId());
          return null;
        }

        collection.addObjects(listCollectionObjects(collection.getId()));

        return collection;
      }
      else
        return null;

    } finally {
      closeDbStuff(result, p);
    }

  }


  /**
   * Fetch the collection defined by <code>bucketName</code> , <code>key</code> & <code>version</code>
   * @param bucketName a single String representing the bucket name where the collection is stored
   * @param key a single String identifying the collection key
   * @param version an integer used to filter the collection regarding the version property
   * @param tag a single String used to filter the collections regarding the tag property. if tag = null, no filter is needed.
   * @param versionChecksum a single string used to filter the collection regarding the checksum property
   * @return {@link org.plos.repo.models.Collection}
   * @throws SQLException
   */
  public Collection getCollection(String bucketName, String key, Integer version, String tag, Integer versionChecksum) throws SQLException {

    PreparedStatement p = null;
    ResultSet result = null;

    try {

      StringBuilder query = new StringBuilder();
      query.append("SELECT * FROM collections a, buckets b WHERE a.bucketId = b.bucketId AND b.bucketName=? AND collKey=? ");

      if (version != null){
        query.append(" AND versionNumber=?");
      }
      if (versionChecksum != null){
        query.append(" AND versionChecksum=?");
      }
      if (tag != null){
        query.append(" AND tag=?");
      }

      query.append(" ORDER BY versionNumber DESC LIMIT 1");

      p = connectionLocal.get().prepareStatement(query.toString());

      p.setString(1, bucketName);
      p.setString(2, key);

      int i = 3;
      if (version != null){
        p.setInt(i++, version);
      }
      if (versionChecksum != null){
        p.setInt(i++, versionChecksum);
      }
      if (tag != null){
        p.setString(i++, tag);
      }

      result = p.executeQuery();

      if (result.next()) {
        Collection collection = mapCollectionRow(result);

        if (collection.getStatus() == Status.DELETED) {
          log.info("searched for collection which has been deleted. id: " + collection.getId());
          return null;
        }

        collection.addObjects(listCollectionObjects(collection.getId()));

        return collection;
      }
      else
        return null;

    } finally {
      closeDbStuff(result, p);
    }

  }

  /**
   * List all versions for the given <code>collection</code>
   * @param collection a single {@link org.plos.repo.models.Collection}
   * @return a list of {@link org.plos.repo.models.Collection}
   * @throws SQLException
   */
  public List<Collection> listCollectionVersions(Collection collection) throws SQLException {

    List<Collection> collections = new ArrayList<Collection>();

    PreparedStatement p = null;
    ResultSet result = null;

    try {

      p = connectionLocal.get().prepareStatement("SELECT * FROM collections c, buckets b WHERE c.bucketId = b.bucketId AND b.bucketName=? AND c.collKey=? AND c.status=? ORDER BY versionNumber ASC");

      p.setString(1, collection.getBucketName());
      p.setString(2, collection.getKey());
      p.setInt(3, Status.USED.getValue());

      result = p.executeQuery();

      while (result.next()) {
        Collection c = mapCollectionRow(result);
        c.addObjects(listCollectionObjects(collection.getId()));
        collections.add(c);
      }

      return collections;

    } finally {
      closeDbStuff(result, p);
    }
  }


  /**
   * Marks the collection defined by <code>key</code> , <code>bucketName</code> & <code>versionNumber</code> as deleted.
   * @param key a single String identifying the collection key
   * @param bucketName a single String identifying the bucket name where the collection is.
   * @param versionNumber an int value representing the version number of the collection.
   * @return an int value indicating the number of updated rows
   * @throws SQLException
   */
  public int markCollectionDeleted(String key, String bucketName, Integer versionNumber, String tag, Integer versionChecksum) throws SQLException {

    PreparedStatement p = null;

    Bucket bucket = getBucket(bucketName);

    if (bucket == null)
      return 0;

    try {

      StringBuffer query = new StringBuffer();
      query.append("UPDATE collections SET status=? WHERE collKey=? AND bucketId=?");

      if (versionNumber != null){
        query.append(" AND versionNumber=?");
      }
      if (versionChecksum != null){
        query.append(" AND versionChecksum=?");
      }
      if (tag != null){
        query.append(" AND tag=?");
      }

      p = connectionLocal.get().prepareStatement(query.toString());

      p.setInt(1, Status.DELETED.getValue());
      p.setString(2, key);
      p.setInt(3, bucket.bucketId);

      int i = 4;
      if (versionNumber != null){
        p.setInt(i++, versionNumber);
      }
      if (versionChecksum != null){
        p.setInt(i++, versionChecksum);
      }
      if (tag != null){
        p.setString(i++, tag);
      }

      return p.executeUpdate();

    } finally {
      closeDbStuff(null, p);
    }

  }

  public List<Object> listObjectVersions(Object object) throws SQLException {

    List<Object> objects = new ArrayList<>();

    PreparedStatement p = null;
    ResultSet result = null;

    try {

      p = connectionLocal.get().prepareStatement("SELECT * FROM objects a, buckets b WHERE a.bucketId = b.bucketId AND bucketName=? AND objKey=? AND status=? ORDER BY versionNumber ASC");

      p.setString(1, object.bucketName);
      p.setString(2, object.key);
      p.setInt(3, Status.USED.getValue()); // TODO: make this in input a parameter?

      result = p.executeQuery();

      while (result.next()) {
        objects.add(mapObjectRow(result));
      }

      return objects;

    } finally {
      closeDbStuff(result, p);
    }
  }

  public int insertCollection(Collection collection) throws SQLException {

    PreparedStatement p = null;
    ResultSet keys = null;

    try {
      p =
          connectionLocal.get().prepareStatement("INSERT INTO collections (bucketId, collkey, timestamp, status, versionNumber, tag, creationDate, versionChecksum) VALUES (?,?,?,?,?,?,?,?)",
              Statement.RETURN_GENERATED_KEYS);

      p.setInt(1, collection.getBucketId());
      p.setString(2, collection.getKey());
      p.setTimestamp(3, collection.getTimestamp());
      p.setInt(4, collection.getStatus().getValue());
      p.setInt(5, collection.getVersionNumber());
      p.setString(6, collection.getTag());
      p.setTimestamp(7, collection.getCreationDate());
      p.setInt(8, collection.getVersionChecksum());

      p.executeUpdate();
      keys = p.getGeneratedKeys();

      if (keys.next()){
        return keys.getInt(1);
      }

      return -1;
    } finally {
      closeDbStuff(keys, p);
    }


  }

  public Boolean existsActiveCollectionForObject(String objKey, String bucketName, Integer version, Integer versionChecksum, String tag) throws SQLException {

    Object object = this.getObject(bucketName, objKey, version, versionChecksum, tag);

    if (object == null){
      return false;
    }

    PreparedStatement p = null;
    ResultSet result = null;

    try{
      p =
          connectionLocal.get().prepareStatement("SELECT * FROM collectionObject co, collections c WHERE c.id = co.collectionId AND co.objectId =? AND c.status = 0");

      p.setInt(1, object.id);

      result = p.executeQuery();

      if (result.next()) {
       return true;

      }

      return false;

    } finally {
      closeDbStuff(result, p);
    }

  }

  public int insertCollectionObjects(Integer collectionId, String objectKey, String bucketName, Integer objectChecksum) throws SQLException {

    PreparedStatement p = null;
    ResultSet result = null;

    try{
      p =
          connectionLocal.get().prepareStatement("SELECT * FROM objects a, buckets b WHERE a.bucketId = b.bucketId AND b.bucketName=? AND objKey=? AND versionChecksum=?");

      p.setString(1, bucketName);
      p.setString(2, objectKey);
      p.setInt(3, objectChecksum);

      result = p.executeQuery();
      Integer objId = null;
      if (result.next()) {
        objId = result.getInt("ID");

        p = connectionLocal.get().prepareStatement("INSERT INTO collectionObject (collectionId, objectId) VALUES (?,?)");

        p.setInt(1, collectionId);
        p.setInt(2, objId);

        return p.executeUpdate();

      }

      return 0;

    } finally {
      closeDbStuff(result, p);
    }

  }

}
