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
import org.plos.repo.models.InputObject;
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
    return new org.plos.repo.models.Object(rs.getInt("ID"), rs.getString("OBJKEY"), rs.getString("CHECKSUM"), rs.getTimestamp("TIMESTAMP"), rs.getString("DOWNLOADNAME"), rs.getString("CONTENTTYPE"), rs.getLong("SIZE"), rs.getString("TAG"), rs.getInt("BUCKETID"), rs.getString("BUCKETNAME"), rs.getInt("VERSIONNUMBER"), Object.STATUS_VALUES.get(rs.getInt("STATUS")));
  }

  private static org.plos.repo.models.Collection mapCollectionRow(ResultSet rs) throws SQLException {
    return new org.plos.repo.models.Collection(rs.getInt("ID"), rs.getString("COLLKEY"), rs.getTimestamp("TIMESTAMP"),
                                               rs.getInt("BUCKETID"), rs.getString("BUCKETNAME"), rs.getInt("VERSIONNUMBER"),
                                                Collection.STATUS_VALUES.get(rs.getInt("STATUS")), rs.getString("TAG"));
  }

  public static Bucket mapBucketRow(ResultSet rs) throws SQLException {
    return new Bucket(rs.getInt("BUCKETID"), rs.getString("BUCKETNAME"));
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

  public int markObjectDeleted(String key, String bucketName, int versionNumber) throws SQLException {

    PreparedStatement p = null;

    Bucket bucket = getBucket(bucketName);

    if (bucket == null)
      return 0;

    try {

      p = connectionLocal.get().prepareStatement("UPDATE objects SET status=? WHERE objKey=? AND bucketId=? AND versionNumber=?");

      p.setInt(1, Object.Status.DELETED.getValue());
      p.setString(2, key);
      p.setInt(3, bucket.bucketId);
      p.setInt(4, versionNumber);

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

      p = connectionLocal.get().prepareStatement("SELECT * FROM objects a, buckets b WHERE a.bucketId = b.bucketId AND b.bucketName=? AND objKey=? AND status=? ORDER BY versionNumber DESC LIMIT 1");

      p.setString(1, bucketName);
      p.setString(2, key);
      p.setInt(3, Object.Status.USED.getValue());

      result = p.executeQuery();

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

    } finally {
      closeDbStuff(result, p);
    }

  }

  public Object getObject(String bucketName, String key, Integer version) throws SQLException {

    PreparedStatement p = null;
    ResultSet result = null;

    try {

      p = connectionLocal.get().prepareStatement("SELECT * FROM objects a, buckets b WHERE a.bucketId = b.bucketId AND b.bucketName=? AND objKey=? AND versionNumber=?");

      p.setString(1, bucketName);
      p.setString(2, key);
      p.setInt(3, version);

      result = p.executeQuery();

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

    } finally {
      closeDbStuff(result, p);
    }

  }

  public int insertObject(Object object) throws SQLException {

    // TODO: return object or objectid from this function?

    PreparedStatement p = null;

    try {

      p = connectionLocal.get().prepareStatement("INSERT INTO objects (objKey, checksum, timestamp, bucketId, contentType, downloadName, size, tag, versionNumber, status) VALUES (?,?,?,?,?,?,?,?,?,?)");

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
        p.setInt(++index, Object.Status.USED.getValue());
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

  public boolean insertBucket(Bucket bucket) throws SQLException {

    int result;

    PreparedStatement p = null;

    try {

      p = connectionLocal.get().prepareStatement("INSERT INTO buckets (bucketName) VALUES(?)");

      p.setString(1, bucket.bucketName);

      result = p.executeUpdate();

    } finally {
      closeDbStuff(null, p);
    }

    return (result > 0);
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

  public List<Object> listObjects(String bucketName, Integer offset, Integer limit, boolean includeDeleted) throws SQLException {

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
      if (limit != null)
        q.append(" LIMIT " + limit);
      if (offset != null)
        q.append(" OFFSET " + offset);
      p = connectionLocal.get().prepareStatement(q.toString());

      int i = 1;

      if (!includeDeleted)
        p.setInt(i++, Object.Status.USED.getValue());

      if (bucketName != null)
        p.setString(i++, bucketName);

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
        p.setInt(i++, Object.Status.USED.getValue());

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
   * Fetch the lastest version of the collection defined by <code>bucketName</code> & <code>key</code>
   * @param bucketName a single String representing the bucket name where the collection is stored
   * @param key a single String identifying the collection key
   * @param tag a single String used to filter the collections regarding the tag property. If tag = null, no filter is needed.
   * @return {@link org.plos.repo.models.Collection}
   * @throws SQLException
   */
  public Collection getCollection(String bucketName, String key, String tag) throws SQLException {

    PreparedStatement p = null;
    ResultSet result = null;

    try {

      StringBuilder query = new StringBuilder();
      query.append("SELECT * FROM collections a, buckets b WHERE a.bucketId = b.bucketId AND b.bucketName=? AND collKey=? AND status=?");
      if (tag != null){
        query.append(" AND tag=?");
      }
      query.append(" ORDER BY versionNumber DESC LIMIT 1");

      p = connectionLocal.get().prepareStatement(query.toString());

      p.setString(1, bucketName);
      p.setString(2, key);
      p.setInt(3, Object.Status.USED.getValue());
      if (tag != null){
        p.setString(4,tag);
      }

      result = p.executeQuery();

      if (result.next()) {
        Collection collection = mapCollectionRow(result);

        if (collection.getStatus() == Collection.Status.DELETED) {
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
   * @param version an integer representing the collection version
   * @param tag a single String used to filter the collections regarding the tag property. if tag = null, no filter is needed.
   * @return {@link org.plos.repo.models.Collection}
   * @throws SQLException
   */
  public Collection getCollection(String bucketName, String key, Integer version, String tag) throws SQLException {

    PreparedStatement p = null;
    ResultSet result = null;

    try {



      StringBuilder query = new StringBuilder();
      query.append("SELECT * FROM collections a, buckets b WHERE a.bucketId = b.bucketId AND b.bucketName=? AND collKey=? AND versionNumber=?");
      if (tag != null){
        query.append(" AND tag=?");
      }

      p = connectionLocal.get().prepareStatement(query.toString());

      p.setString(1, bucketName);
      p.setString(2, key);
      p.setInt(3, version);

      if (tag != null){
          p.setString(4, tag);
      }

      result = p.executeQuery();

      if (result.next()) {
        Collection collection = mapCollectionRow(result);

        if (collection.getStatus() == Collection.Status.DELETED) {
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
      p.setInt(3, Collection.Status.USED.getValue());

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
  public int markCollectionDeleted(String key, String bucketName, int versionNumber) throws SQLException {

    PreparedStatement p = null;

    Bucket bucket = getBucket(bucketName);

    if (bucket == null)
      return 0;

    try {

      p = connectionLocal.get().prepareStatement("UPDATE collections SET status=? WHERE collKey=? AND bucketId=? AND versionNumber=?");

      p.setInt(1, Collection.Status.DELETED.getValue());
      p.setString(2, key);
      p.setInt(3, bucket.bucketId);
      p.setInt(4, versionNumber);

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
      p.setInt(3, Object.Status.USED.getValue()); // TODO: make this in input a parameter?

      result = p.executeQuery();

      while (result.next()) {
        objects.add(mapObjectRow(result));
      }

      return objects;

    } finally {
      closeDbStuff(result, p);
    }
  }

  public int insertCollection(Collection collection, List<InputObject> inputObjects) throws SQLException {

    PreparedStatement p = null;
    ResultSet keys = null;

    try {
      p =
          connectionLocal.get().prepareStatement("INSERT INTO collections (bucketId, collkey, timestamp, status, versionNumber, tag) VALUES (?,?,?,?,?,?)",
              Statement.RETURN_GENERATED_KEYS);

      p.setInt(1, collection.getBucketId());
      p.setString(2, collection.getKey());
      p.setTimestamp(3, collection.getTimestamp());
      p.setInt(4, collection.getStatus().getValue());
      p.setInt(5, collection.getVersionNumber());
      p.setString(6, collection.getTag());

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

  public int insertCollectionObjects(Integer collectionId, String objectKey, String bucketName, Integer objectVersion) throws SQLException {

    PreparedStatement p = null;
    ResultSet result = null;

    try{
      p =
          connectionLocal.get().prepareStatement("SELECT * FROM objects a, buckets b WHERE a.bucketId = b.bucketId AND b.bucketName=? AND objKey=? AND versionNumber=?");

      p.setString(1, bucketName);
      p.setString(2, objectKey);
      p.setInt(3, objectVersion);

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
