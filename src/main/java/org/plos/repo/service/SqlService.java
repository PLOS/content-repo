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

import org.plos.repo.models.Audit;
import org.plos.repo.models.Bucket;
import org.plos.repo.models.Operation;
import org.plos.repo.models.RepoCollection;
import org.plos.repo.models.RepoObject;
import org.plos.repo.models.Status;
import org.plos.repo.util.UUIDFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public abstract class SqlService {

  private static final Logger log = LoggerFactory.getLogger(SqlService.class);

  protected DataSource dataSource;

  private static final ThreadLocal<Connection> connectionLocal = new ThreadLocal<>();

  private static final String OBJECT_KEY_COLUMN = "OBJKEY";
  private static final String COLLECTION_KEY_COLUMN = "COLLKEY";
  private static final String BUCKET_ID_COLUMN = "BUCKETID";
  private static final String BUCKET_NAME_COLUMN = "BUCKETNAME";
  private static final String STATUS_COLUMN = "STATUS";
  private static final String ID_COLUMN = "ID";
  private static final String CHECKSUM_COLUMN = "CHECKSUM";
  private static final String TIMESTAMP_COLUMN = "TIMESTAMP";
  private static final String TAG_COLUMN = "TAG";
  private static final String VERSION_NUMBER_COLUMN = "VERSIONNUMBER";
  private static final String CREATION_DATE_COLUMN = "CREATIONDATE";
  private static final String USER_METADATA_COLUMN = "USERMETADATA";
  private static final String UUID_COLUMN = "UUID";
  private static final String DOWNLOAD_NAME_COLUMN = "DOWNLOADNAME";
  private static final String CONTENT_TYPE_COLUMN = "CONTENTTYPE";
  private static final String SIZE_COLUMN = "SIZE";
  private static final String KEY_VALUE_COLUMN = "KEYVALUE";
  private static final String OPERATION_COLUMN = "OPERATION";

  private static String OBJECT_COLUMNS = "obj." + OBJECT_KEY_COLUMN + ", obj." + BUCKET_ID_COLUMN
      + ", obj." + STATUS_COLUMN + ", obj." + ID_COLUMN + ", obj." + CHECKSUM_COLUMN
      + ", obj." + TIMESTAMP_COLUMN + ", obj." + DOWNLOAD_NAME_COLUMN + ", obj." + CONTENT_TYPE_COLUMN
      + ", obj." + SIZE_COLUMN + ", obj." + TAG_COLUMN + ", obj." + VERSION_NUMBER_COLUMN + ", obj." + CREATION_DATE_COLUMN
      + ", obj." + USER_METADATA_COLUMN + " , obj." + UUID_COLUMN;

  private static String COLLECTION_COLUMNS = "c." + COLLECTION_KEY_COLUMN + ", c." + BUCKET_ID_COLUMN
      + ", c." + STATUS_COLUMN + ", c." + ID_COLUMN + ", c." + TIMESTAMP_COLUMN
      + ", c." + TAG_COLUMN + ", c." + VERSION_NUMBER_COLUMN + ", c." + CREATION_DATE_COLUMN
      + ", c." + USER_METADATA_COLUMN + ", c." + UUID_COLUMN;

  @Required
  public void setDataSource(DataSource dataSource) throws SQLException {
    this.dataSource = dataSource;
    postDbInit();
  }

  public abstract void postDbInit() throws SQLException;

  private static RepoObject mapObjectRow(ResultSet rs) throws SQLException, RepoException {
    RepoObject repoObject = new RepoObject(rs.getString(OBJECT_KEY_COLUMN), rs.getInt(BUCKET_ID_COLUMN),
        rs.getString(BUCKET_NAME_COLUMN), Status.STATUS_VALUES.get(rs.getInt(STATUS_COLUMN)));
    repoObject.setId(rs.getInt(ID_COLUMN));
    repoObject.setChecksum(rs.getString(CHECKSUM_COLUMN));
    repoObject.setTimestamp(rs.getTimestamp(TIMESTAMP_COLUMN));
    repoObject.setDownloadName(rs.getString(DOWNLOAD_NAME_COLUMN));
    repoObject.setContentType(rs.getString(CONTENT_TYPE_COLUMN));
    repoObject.setSize(rs.getLong(SIZE_COLUMN));
    repoObject.setTag(rs.getString(TAG_COLUMN));
    repoObject.setVersionNumber(rs.getInt(VERSION_NUMBER_COLUMN));
    repoObject.setCreationDate(rs.getTimestamp(CREATION_DATE_COLUMN));
    repoObject.setUserMetadata(rs.getString(USER_METADATA_COLUMN));
    repoObject.setUuid(UUIDFormatter.getUuid(rs.getString(UUID_COLUMN)));

    return repoObject;
  }

  private static RepoCollection mapCollectionRow(ResultSet rs) throws SQLException, RepoException {
    RepoCollection collection = new RepoCollection(rs.getString(COLLECTION_KEY_COLUMN), rs.getInt(BUCKET_ID_COLUMN),
        rs.getString(BUCKET_NAME_COLUMN), Status.STATUS_VALUES.get(rs.getInt(STATUS_COLUMN)));
    collection.setId(rs.getInt(ID_COLUMN));
    collection.setTimestamp(rs.getTimestamp(TIMESTAMP_COLUMN));
    collection.setVersionNumber(rs.getInt(VERSION_NUMBER_COLUMN));
    collection.setTag(rs.getString(TAG_COLUMN));
    collection.setCreationDate(rs.getTimestamp(CREATION_DATE_COLUMN));
    collection.setUserMetadata(rs.getString(USER_METADATA_COLUMN));
    collection.setUuid(UUIDFormatter.getUuid(rs.getString(UUID_COLUMN)));
    return collection;
  }

  private static Audit mapAuditRow(ResultSet rs) throws SQLException, RepoException {
    return new Audit.AuditBuilder(rs.getString("BUCKETNAME"), Operation.OPERATION_VALUES.get(rs.getString("OPERATION")))
        .setKey(rs.getString("KEYVALUE"))
        .setUuid(UUIDFormatter.getUuid(rs.getString(UUID_COLUMN)))
        .setId(rs.getInt("ID"))
        .setTimestamp(rs.getTimestamp("TIMESTAMP"))
        .build();
  }

  public static Bucket mapBucketRow(ResultSet rs) throws SQLException {
    return new Bucket(rs.getInt(BUCKET_ID_COLUMN),
        rs.getString(BUCKET_NAME_COLUMN),
        rs.getTimestamp(TIMESTAMP_COLUMN),
        rs.getTimestamp(CREATION_DATE_COLUMN));
  }

  private static void closeDbStuff(ResultSet result, PreparedStatement p) throws SQLException {
    if (result != null) {
      result.close();
    }

    if (p != null) {
      p.close();
    }
  }

  /**
   * Set a connection in the ThreadLocal. The autocommit for the connection is disable, meaning that when using this
   * method, there's a need to commit or rollback the transaction.
   *
   * @throws SQLException
   */
  public void getConnection() throws SQLException {
    Connection dbConnection = dataSource.getConnection();
    dbConnection.setAutoCommit(false);
    connectionLocal.set(dbConnection);
  }

  /**
   * Set a connection in the ThreadLocal. The autocommit for the connection is set to true, meaning that when using this
   * method, there's no need to perform commit. This method is intended to be used for read operations.
   *
   * @throws SQLException
   */
  public void getReadOnlyConnection() throws SQLException {
    Connection dbConnection = dataSource.getConnection();
    dbConnection.setAutoCommit(true);
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

      if (result.next()) {
        return mapBucketRow(result);
      } else {
        return null;
      }
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

  public int markObjectPurged(String key, String bucketName, Integer version, UUID uuid, String tag) throws SQLException {
    return markObject(key, bucketName, version, uuid, tag, Status.PURGED);
  }

  public int markObjectDeleted(String key, String bucketName, Integer version, UUID uuid, String tag) throws SQLException {
    return markObject(key, bucketName, version, uuid, tag, Status.DELETED);
  }

  private int markObject(String key, String bucketName, Integer version, UUID uuid, String tag, Status status) throws SQLException {
    PreparedStatement p = null;

    Bucket bucket = getBucket(bucketName);

    if (bucket == null) {
      return 0;
    }

    try {
      StringBuilder query = new StringBuilder();
      query.append("UPDATE objects SET status=? WHERE objKey=? AND bucketId=?");

      if (version != null) {
        query.append(" AND versionNumber=?");
      }
      if (uuid != null) {
        query.append(" AND uuid = ?");
      }
      if (tag != null) {
        query.append(" AND tag=?");
      }

      p = connectionLocal.get().prepareStatement(query.toString());

      p.setInt(1, status.getValue());
      p.setString(2, key);
      p.setInt(3, bucket.getBucketId());

      int i = 4;
      if (version != null) {
        p.setInt(i++, version);
      }
      if (uuid != null) {
        p.setString(i++, uuid.toString());
      }
      if (tag != null) {
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

      if (result.next()) {
        return result.getInt("versionNumber") + 1;
      } else {
        return 0;
      }
    } finally {
      closeDbStuff(result, p);
    }
  }

  public Integer getCollectionNextAvailableVersion(String bucketName, String key) throws SQLException {
    return getNextAvailableVersionNumber(bucketName, key, "collections", "collKey");
  }

  public Integer getObjectNextAvailableVersion(String bucketName, String key) throws SQLException {
    return getNextAvailableVersionNumber(bucketName, key, "objects", "objKey");
  }

  public RepoObject getObject(String bucketName, String key) throws SQLException, RepoException {
    PreparedStatement p = null;
    ResultSet result = null;

    try {
      p = connectionLocal.get().prepareStatement("SELECT " + OBJECT_COLUMNS + ", b.BUCKETNAME  FROM objects obj, buckets b " +
          "WHERE obj.bucketId = b.bucketId AND b.bucketName=? AND obj.objKey=? AND status=? ORDER BY obj.creationDate " +
          "DESC LIMIT 1");

      p.setString(1, bucketName);
      p.setString(2, key);
      p.setInt(3, Status.USED.getValue());

      result = p.executeQuery();

      if (result.next()) {
        RepoObject repoObject = mapObjectRow(result);

        if (repoObject.getStatus() == Status.DELETED || repoObject.getStatus() == Status.PURGED) {
          log.info("searched for object which has been deleted/purged. id: " + repoObject.getId());
          return null;
        }

        return repoObject;
      } else {
        return null;
      }
    } finally {
      closeDbStuff(result, p);
    }
  }

  public RepoObject getObject(String bucketName, String key, Integer version, UUID uuid, String tag) throws SQLException, RepoException {
    PreparedStatement p = null;
    ResultSet result = null;

    try {
      StringBuilder query = new StringBuilder();
      query.append("SELECT " + OBJECT_COLUMNS + ", b.BUCKETNAME FROM objects obj, buckets b " +
          "WHERE obj.bucketId = b.bucketId AND b.bucketName=? AND obj.objKey=?");

      if (version != null) {
        query.append(" AND versionNumber=?");
      }
      if (uuid != null) {
        query.append(" AND uuid = ?");
      }
      if (tag != null) {
        query.append(" AND tag=?");
      }

      query.append(" ORDER BY obj.creationDate DESC LIMIT 1");

      p = connectionLocal.get().prepareStatement(query.toString());

      p.setString(1, bucketName);
      p.setString(2, key);

      int i = 3;
      if (version != null) {
        p.setInt(i++, version);
      }
      if (uuid != null) {
        p.setString(i++, uuid.toString());
      }
      if (tag != null) {
        p.setString(i++, tag);
      }

      result = p.executeQuery();

      if (result.next()) {
        RepoObject repoObject = mapObjectRow(result);

        if (repoObject.getStatus() == Status.DELETED || repoObject.getStatus() == Status.PURGED) {
          log.info("searched for object which has been deleted/purged. id: " + repoObject.getId());
          return null;
        }

        return repoObject;
      } else {
        return null;
      }
    } finally {
      closeDbStuff(result, p);
    }
  }

  public RepoObject getObject(String bucketName, String key, Integer version, UUID uuid,
                              String tag, boolean searchInDeleted, boolean searchInPurged) throws SQLException, RepoException {
    PreparedStatement p = null;
    ResultSet result = null;

    StringBuilder query = new StringBuilder();
    query.append("SELECT " + OBJECT_COLUMNS + ", b.BUCKETNAME FROM objects obj, buckets b " +
        "WHERE obj.bucketId = b.bucketId AND b.bucketName=? AND obj.objKey=?");

    if (version != null) {
      query.append(" AND versionNumber=?");
    }
    if (uuid != null) {
      query.append(" AND uuid = ?");
    }
    if (tag != null) {
      query.append(" AND tag=?");
    }
    if (!searchInDeleted && !searchInPurged) {
      query.append(" AND status=?");
    }
    if ((!searchInDeleted && searchInPurged) || (searchInDeleted && !searchInPurged)) {
      query.append(" AND status in (?,?)");
    }

    query.append(" ORDER BY obj.creationDate DESC LIMIT 1");

    try {
      p = connectionLocal.get().prepareStatement(query.toString());

      p.setString(1, bucketName);
      p.setString(2, key);

      int i = 3;
      if (version != null) {
        p.setInt(i++, version);
      }
      if (uuid != null) {
        p.setString(i++, uuid.toString());
      }
      if (tag != null) {
        p.setString(i++, tag);
      }

      if ((!searchInDeleted && !searchInPurged) ||
          (!searchInDeleted && searchInPurged) || (searchInDeleted && !searchInPurged)) {
        p.setInt(i++, Status.USED.getValue());
        if (searchInDeleted) {
          p.setInt(i++, Status.DELETED.getValue());
        } else if (searchInPurged) {
          p.setInt(i++, Status.PURGED.getValue());
        }
      }

      result = p.executeQuery();

      if (result.next()) {
        RepoObject repoObject = mapObjectRow(result);

        return repoObject;
      } else {
        return null;
      }
    } finally {
      closeDbStuff(result, p);
    }
  }

  public int insertObject(RepoObject repoObject) throws SQLException {
    // TODO: return object or objectid from this function?

    PreparedStatement p = null;

    try {
      p = connectionLocal.get().prepareStatement("INSERT INTO objects (objKey, checksum, timestamp, bucketId, contentType, downloadName, size, " +
          "tag, versionNumber, status, creationDate, userMetadata, uuid) " +
          "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)");


      p.setString(1, repoObject.getKey());
      p.setString(2, repoObject.getChecksum());
      p.setTimestamp(3, repoObject.getTimestamp());
      p.setInt(4, repoObject.getBucketId());
      p.setString(5, repoObject.getContentType());
      p.setString(6, repoObject.getDownloadName());
      p.setLong(7, repoObject.getSize());
      p.setString(8, repoObject.getTag());
      p.setInt(9, repoObject.getVersionNumber());
      p.setInt(10, repoObject.getStatus().getValue());
      p.setTimestamp(11, repoObject.getTimestamp());
      p.setString(12, repoObject.getUserMetadata());
      p.setString(13, repoObject.getUuid().toString());

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
      if (!includeDeleted) {
        q.append(" AND a.status=?");
      }
      if (bucketName != null) {
        q.append(" AND bucketName=?");
      }

      p = connectionLocal.get().prepareStatement(q.toString());

      int index = 0;
      if (!includeDeleted) {
        p.setInt(++index, Status.USED.getValue());
      }
      if (bucketName != null) {
        p.setString(++index, bucketName);
      }

      result = p.executeQuery();

      if (result.next()) {
        return result.getInt(1);
      } else {
        return null;
      }
    } finally {
      closeDbStuff(result, p);
    }
  }

  public boolean insertBucket(Bucket bucket, Timestamp creationDate) throws SQLException {
    int result;

    PreparedStatement p = null;

    try {
      p = connectionLocal.get().prepareStatement("INSERT INTO buckets (bucketName, timestamp, creationDate) " +
          "VALUES(?, ?, ?)");

      p.setString(1, bucket.getBucketName());
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

  public List<Bucket> getObjectsSize(String bucketName) throws SQLException {
    List<Bucket> buckets = new ArrayList<>();

    PreparedStatement p = null;
    ResultSet result = null;

    try {
      p = connectionLocal.get().prepareStatement("");

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

  public List<RepoObject> listObjects(String bucketName, Integer offset, Integer limit, boolean includeDeleted, boolean includePurge, String tag) throws SQLException, RepoException {
    List<RepoObject> repoObjects = new ArrayList<>();

    PreparedStatement p = null;
    ResultSet result = null;

    try {
      StringBuilder q = new StringBuilder();
      q.append("SELECT " + OBJECT_COLUMNS + ", b.BUCKETNAME FROM objects as obj, buckets as b " +
          "WHERE obj.bucketId = b.bucketId");

      if (!includeDeleted && !includePurge) {
        q.append(" AND status=?");
      }
      if ((includeDeleted && !includePurge) || (!includeDeleted && includePurge)) {
        q.append(" AND status in(?,?)");
      }
      if (bucketName != null) {
        q.append(" AND bucketName=?");
      }
      if (tag != null) {
        q.append(" AND TAG=?");
      }
      if (limit != null) {
        q.append(" LIMIT " + limit);
      }
      if (offset != null) {
        q.append(" OFFSET " + offset);
      }
      p = connectionLocal.get().prepareStatement(q.toString());


      int i = 1;

      if (!includeDeleted && !includePurge) {
        p.setInt(i++, Status.USED.getValue());
      }
      if (includeDeleted && !includePurge) {
        p.setInt(i++, Status.USED.getValue());
        p.setInt(i++, Status.DELETED.getValue());
      }
      if (!includeDeleted && includePurge) {
        p.setInt(i++, Status.USED.getValue());
        p.setInt(i++, Status.PURGED.getValue());
      }

      if (bucketName != null) {
        p.setString(i++, bucketName);
      }

      if (tag != null) {
        p.setString(i++, tag);
      }

      result = p.executeQuery();

      while (result.next()) {
        repoObjects.add(mapObjectRow(result));
      }

      return repoObjects;
    } finally {
      closeDbStuff(result, p);
    }
  }

  public List<RepoObject> listObjects(Timestamp timestamp) throws SQLException, RepoException {
    List<RepoObject> repoObjects = new ArrayList<>();

    PreparedStatement p = null;
    ResultSet result = null;

    try {
      StringBuilder q = new StringBuilder();
      q.append("SELECT " + OBJECT_COLUMNS + " FROM objects obj WHERE timestamp >= ?");

      p.setTimestamp(1, timestamp);

      result = p.executeQuery();

      while (result.next()) {
        repoObjects.add(mapObjectRow(result));
      }

      return repoObjects;
    } finally {
      closeDbStuff(result, p);
    }
  }

  /**
   * Returns a list of collections for the given bucket name <code>bucketName</code>. For each collection, it will
   * return the meta data and the list of object contain in the collection.
   *
   * @param bucketName     a single String representing the bucket name where the collection is stored
   * @param offset         an Integer used to determine the offset of the response
   * @param limit          an Integer used to determine the limit of the response
   * @param includeDeleted a boolean used to define is the response will include delete collections or not
   * @param tag            a single String used to filter the collections regarding the tag property
   * @return a list of {@link org.plos.repo.models.RepoCollection}
   * @throws SQLException
   */
  public List<RepoCollection> listCollections(String bucketName, Integer offset, Integer limit, Boolean includeDeleted, String tag) throws SQLException, RepoException {
    List<RepoCollection> repoCollections = new ArrayList<>();

    PreparedStatement p = null;
    ResultSet result = null;

    try {
      p = connectionLocal.get().prepareStatement(
          getCollectionMetadataQuery(bucketName, offset, limit, includeDeleted, tag));

      int i = 1;
      if (!includeDeleted) {
        p.setInt(i++, Status.USED.getValue());
      }

      if (bucketName != null) {
        p.setString(i++, bucketName);
      }

      if (tag != null) {
        p.setString(i++, tag);
      }

      result = p.executeQuery();

      while (result.next()) {
        RepoCollection c = mapCollectionRow(result);
        c.addObjects(listCollectionObjects(c.getId()));
        repoCollections.add(c);
      }

      return repoCollections;
    } finally {
      closeDbStuff(result, p);
    }
  }

  /**
   * Returns a list of collections meta data for the given bucket name <code>bucketName</code>
   *
   * @param bucketName     a single String representing the bucket name where the collection is stored
   * @param offset         an Integer used to determine the offset of the response
   * @param limit          an Integer used to determine the limit of the response
   * @param includeDeleted a boolean used to define is the response will include delete collections or not
   * @param tag            a single String used to filter the collections regarding the tag property
   * @return a list of {@link org.plos.repo.models.RepoCollection}
   * @throws SQLException
   */
  public List<RepoCollection> listCollectionsMetaData(String bucketName, Integer offset, Integer limit, Boolean includeDeleted, String tag) throws SQLException, RepoException {
    List<RepoCollection> repoCollections = new ArrayList<>();

    PreparedStatement p = null;
    ResultSet result = null;

    try {
      p = connectionLocal.get().prepareStatement(getCollectionMetadataQuery(bucketName, offset, limit, includeDeleted, tag));

      int i = 1;
      if (!includeDeleted) {
        p.setInt(i++, Status.USED.getValue());
      }

      if (bucketName != null) {
        p.setString(i++, bucketName);
      }

      if (tag != null) {
        p.setString(i++, tag);
      }

      result = p.executeQuery();

      while (result.next()) {
        RepoCollection c = mapCollectionRow(result);
        repoCollections.add(c);
      }

      return repoCollections;
    } finally {
      closeDbStuff(result, p);
    }
  }

  private String getCollectionMetadataQuery(String bucketName, Integer offset, Integer limit, boolean includeDeleted, String tag) {
    StringBuilder q = new StringBuilder();
    q.append("SELECT " + COLLECTION_COLUMNS + ", b.BUCKETNAME " +
        "FROM collections c, buckets b WHERE c.bucketId = b.bucketId");

    if (!includeDeleted) {
      q.append(" AND status=?");
    }
    if (bucketName != null) {
      q.append(" AND bucketName=?");
    }
    if (tag != null) {
      q.append(" AND TAG=?");
    }
    if (limit != null) {
      q.append(" LIMIT " + limit);
    }
    if (offset != null) {
      q.append(" OFFSET " + offset);
    }

    return q.toString();
  }

  public List<RepoCollection> listCollections(Timestamp timestamp) throws SQLException, RepoException {
    List<RepoCollection> repoCollections = new ArrayList<>();

    PreparedStatement p = null;
    ResultSet result = null;

    try {
      StringBuilder q = new StringBuilder();
      q.append("SELECT " + COLLECTION_COLUMNS + " FROM collections WHERE c.timestamp > ?");

      p = connectionLocal.get().prepareStatement(q.toString());

      p.setTimestamp(1, timestamp);

      while (result.next()) {
        repoCollections.add(mapCollectionRow(result));
      }

      return repoCollections;
    } finally {
      closeDbStuff(result, p);
    }
  }

  /**
   * Returns the list of objects contains in the given collection <code>id</code>
   *
   * @param id an integer representing the collection id
   * @return a list of {@link org.plos.repo.models.RepoObject }
   * @throws SQLException
   */
  protected List<RepoObject> listCollectionObjects(Integer id) throws SQLException, RepoException {
    List<RepoObject> repoObjects = new ArrayList<>();

    PreparedStatement p = null;
    ResultSet result = null;

    try {
      StringBuilder q = new StringBuilder();
      q.append(" SELECT " + OBJECT_COLUMNS + ", b.*\n" +
          "FROM objects obj, collectionObject co, buckets b\n" +
          "WHERE co.collectionId = ?\n" +
          "AND co.objectId = obj.id\n" +
          "AND obj.bucketId = b.bucketId");

      p = connectionLocal.get().prepareStatement(q.toString());

      p.setInt(1, id);

      result = p.executeQuery();

      while (result.next()) {
        repoObjects.add(mapObjectRow(result));
      }

      return repoObjects;
    } finally {
      closeDbStuff(result, p);
    }
  }

  /**
   * Fetch the latest version in used of the collection defined by <code>bucketName</code> & <code>key</code>. The
   * latest version is defined as the latest created collection with status = USED.
   *
   * @param bucketName a single String representing the bucket name where the collection is stored
   * @param key        a single String identifying the collection key
   * @return {@link org.plos.repo.models.RepoCollection}
   * @throws SQLException
   */
  public RepoCollection getCollection(String bucketName, String key) throws SQLException, RepoException {
    PreparedStatement p = null;
    ResultSet result = null;

    try {
      StringBuilder query = new StringBuilder();
      query.append("SELECT " + COLLECTION_COLUMNS + ", b.BUCKETNAME FROM collections c, buckets b " +
          "WHERE c.bucketId = b.bucketId AND b.bucketName=? " +
          "AND collKey=? AND status=? ORDER BY c.creationDate DESC LIMIT 1");

      p = connectionLocal.get().prepareStatement(query.toString());

      p.setString(1, bucketName);
      p.setString(2, key);
      p.setInt(3, Status.USED.getValue());

      result = p.executeQuery();

      if (result.next()) {
        RepoCollection repoCollection = mapCollectionRow(result);

        if (repoCollection.getStatus() == Status.DELETED) {
          log.info("searched for collection which has been deleted. id: " + repoCollection.getId());
          return null;
        }

        repoCollection.addObjects(listCollectionObjects(repoCollection.getId()));

        return repoCollection;
      } else {
        return null;
      }
    } finally {
      closeDbStuff(result, p);
    }
  }


  /**
   * Fetch the collection defined by <code>bucketName</code> , <code>key</code> & <code>version</code>
   *
   * @param bucketName a single String representing the bucket name where the collection is stored
   * @param key        a single String identifying the collection key
   * @param version    an integer used to filter the collection regarding the version property
   * @param tag        a single String used to filter the collections regarding the tag property. if tag = null, no
   *                   filter is needed.
   * @param uuid       a single UUID used to filter the collection regarding the universal id property
   * @return {@link org.plos.repo.models.RepoCollection}
   * @throws SQLException
   */
  public RepoCollection getCollection(String bucketName, String key, Integer version, String tag, UUID uuid) throws SQLException, RepoException {
    PreparedStatement p = null;
    ResultSet result = null;

    try {
      StringBuilder query = new StringBuilder();
      query.append("SELECT " + COLLECTION_COLUMNS + ", b.BUCKETNAME FROM collections c, buckets b " +
          "WHERE c.bucketId = b.bucketId AND b.bucketName=? AND collKey=? ");

      if (version != null) {
        query.append(" AND versionNumber=?");
      }
      if (uuid != null) {
        query.append(" AND uuid= ?");
      }
      if (tag != null) {
        query.append(" AND tag=?");
      }

      query.append(" ORDER BY c.creationDate DESC LIMIT 1");

      p = connectionLocal.get().prepareStatement(query.toString());

      p.setString(1, bucketName);
      p.setString(2, key);

      int i = 3;
      if (version != null) {
        p.setInt(i++, version);
      }
      if (uuid != null) {
        p.setString(i++, uuid.toString());
      }
      if (tag != null) {
        p.setString(i++, tag);
      }

      result = p.executeQuery();

      if (result.next()) {
        RepoCollection repoCollection = mapCollectionRow(result);

        if (repoCollection.getStatus() == Status.DELETED) {
          log.info("searched for collection which has been deleted. id: " + repoCollection.getId());
          return null;
        }

        repoCollection.addObjects(listCollectionObjects(repoCollection.getId()));

        return repoCollection;
      } else {
        return null;
      }
    } finally {
      closeDbStuff(result, p);
    }
  }

  /**
   * List all versions for the given <code>collection</code>
   *
   * @param bucketName a single String identifying the bucket name where the collection is.
   * @param key        a single String identifying the collection key
   * @return a list of {@link org.plos.repo.models.RepoCollection}
   * @throws SQLException
   */
  public List<RepoCollection> listCollectionVersions(String bucketName, String key) throws SQLException, RepoException {
    List<RepoCollection> repoCollections = new ArrayList<>();

    PreparedStatement p = null;
    ResultSet result = null;

    try {
      p = connectionLocal.get().prepareStatement("SELECT " + COLLECTION_COLUMNS + ", b.BUCKETNAME FROM collections c, buckets b " +
          "WHERE c.bucketId = b.bucketId AND b.bucketName=? AND c.collKey=? AND c.status=? " +
          "ORDER BY versionNumber ASC");

      p.setString(1, bucketName);
      p.setString(2, key);
      p.setInt(3, Status.USED.getValue());

      result = p.executeQuery();

      while (result.next()) {
        RepoCollection c = mapCollectionRow(result);
        c.addObjects(listCollectionObjects(c.getId()));
        repoCollections.add(c);
      }

      return repoCollections;
    } finally {
      closeDbStuff(result, p);
    }
  }


  /**
   * Marks the collection defined by <code>key</code> , <code>bucketName</code> & <code>versionNumber</code> as
   * deleted.
   *
   * @param key        a single String identifying the collection key
   * @param bucketName a single String identifying the bucket name where the collection is.
   * @param uuid       a single UUID used to filter the collection regarding the universal id property
   * @return an int value indicating the number of updated rows
   * @throws SQLException
   */
  public int markCollectionDeleted(String key, String bucketName, Integer versionNumber, String tag, UUID uuid) throws SQLException {
    PreparedStatement p = null;

    Bucket bucket = getBucket(bucketName);

    if (bucket == null) {
      return 0;
    }

    try {
      StringBuilder query = new StringBuilder();
      query.append("UPDATE collections SET status=? WHERE collKey=? AND bucketId=?");

      if (versionNumber != null) {
        query.append(" AND versionNumber=?");
      }
      if (uuid != null) {
        query.append(" AND uuid= ?");
      }
      if (tag != null) {
        query.append(" AND tag=?");
      }

      p = connectionLocal.get().prepareStatement(query.toString());

      p.setInt(1, Status.DELETED.getValue());
      p.setString(2, key);
      p.setInt(3, bucket.getBucketId());

      int i = 4;
      if (versionNumber != null) {
        p.setInt(i++, versionNumber);
      }
      if (uuid != null) {
        p.setString(i++, uuid.toString());
      }
      if (tag != null) {
        p.setString(i++, tag);
      }

      return p.executeUpdate();
    } finally {
      closeDbStuff(null, p);
    }
  }

  public List<RepoObject> listObjectVersions(String bucketName, String objectKey) throws SQLException, RepoException {
    List<RepoObject> repoObjects = new ArrayList<>();

    PreparedStatement p = null;
    ResultSet result = null;

    try {
      p = connectionLocal.get().prepareStatement("SELECT " + OBJECT_COLUMNS + ", b.BUCKETNAME FROM objects obj, buckets b " +
          "WHERE obj.bucketId = b.bucketId AND bucketName=? AND obj.objKey=? AND status=? " +
          "ORDER BY versionNumber ASC");

      p.setString(1, bucketName);
      p.setString(2, objectKey);
      p.setInt(3, Status.USED.getValue());

      result = p.executeQuery();

      while (result.next()) {
        repoObjects.add(mapObjectRow(result));
      }

      return repoObjects;
    } finally {
      closeDbStuff(result, p);
    }
  }

  public int insertCollection(RepoCollection repoCollection) throws SQLException {
    PreparedStatement p = null;
    ResultSet keys = null;

    try {
      p =
          connectionLocal.get().prepareStatement("INSERT INTO collections (bucketId, collkey, timestamp, status, versionNumber, " +
                  "tag, creationDate, userMetadata, uuid) VALUES (?,?,?,?,?,?,?,?,?)",
              Statement.RETURN_GENERATED_KEYS);

      p.setInt(1, repoCollection.getBucketId());
      p.setString(2, repoCollection.getKey());
      p.setTimestamp(3, repoCollection.getTimestamp());
      p.setInt(4, repoCollection.getStatus().getValue());
      p.setInt(5, repoCollection.getVersionNumber());
      p.setString(6, repoCollection.getTag());
      p.setTimestamp(7, repoCollection.getCreationDate());
      p.setString(8, repoCollection.getUserMetadata());
      p.setString(9, repoCollection.getUuid().toString());

      p.executeUpdate();
      keys = p.getGeneratedKeys();

      if (keys.next()) {
        return keys.getInt(1);
      }

      return -1;
    } finally {
      closeDbStuff(keys, p);
    }
  }

  public Boolean existsActiveCollectionForObject(String objKey, String bucketName, Integer version, UUID uuid, String tag) throws SQLException, RepoException {
    RepoObject repoObject = this.getObject(bucketName, objKey, version, uuid, tag);

    if (repoObject == null) {
      return false;
    }

    PreparedStatement p = null;
    ResultSet result = null;

    try {
      p =
          connectionLocal.get().prepareStatement("SELECT * FROM collectionObject co, collections c " +
              "WHERE c.id = co.collectionId AND co.objectId =? AND c.status = 0");

      p.setInt(1, repoObject.getId());

      result = p.executeQuery();

      if (result.next()) {
        return true;
      }

      return false;
    } finally {
      closeDbStuff(result, p);
    }
  }

  public boolean insertCollectionObjects(Integer collectionId, Integer objectId) throws SQLException {
    PreparedStatement p = null;

    try {

        p = connectionLocal.get().prepareStatement("INSERT INTO collectionObject (collectionId, objectId) " +
            "VALUES (?,?)");

        p.setInt(1, collectionId);
        p.setInt(2, objectId);

        return p.executeUpdate() > 0;

    } finally {
      closeDbStuff(null, p);
    }
  }

  public int countUsedAndDeletedObjectsReference(String bucketName, String checksum) throws SQLException {
    PreparedStatement p = null;
    ResultSet result = null;

    StringBuilder q = new StringBuilder("SELECT COUNT(*) FROM objects a, buckets b WHERE a.bucketId = b.bucketId");
    q.append(" AND a.status IN (?,?)");
    q.append(" AND bucketName=?");
    q.append(" AND checksum=?");

    try {
      p = connectionLocal.get().prepareStatement(q.toString());

      p.setInt(1, Status.USED.getValue());
      p.setInt(2, Status.DELETED.getValue());
      p.setString(3, bucketName);
      p.setString(4, checksum);

      result = p.executeQuery();

      if (result.next()) {
        return result.getInt(1);
      } else {
        return 0;
      }
    } finally {
      closeDbStuff(result, p);
    }
  }

  public int removeBucketContent(String bucketName) throws SQLException {
    PreparedStatement p = null;

    try {
      p = connectionLocal.get().prepareStatement("SELECT bucketId FROM buckets WHERE bucketName=?");
      p.setString(1, bucketName);

      ResultSet result = p.executeQuery();
      int bucketId = 0;

      if (result.next()) {
        bucketId = result.getInt(1);
      } else {
        return 0;
      }

      p = connectionLocal.get().prepareStatement("DELETE FROM collectionObject " +
          "WHERE collectionid IN " +
          "(SELECT id FROM collections " +
          "WHERE bucketId=?)");
      p.setInt(1, bucketId);
      p.executeUpdate();

      p = connectionLocal.get().prepareStatement("DELETE FROM objects WHERE bucketId=?");
      p.setInt(1, bucketId);
      p.executeUpdate();

      p = connectionLocal.get().prepareStatement("DELETE FROM collections WHERE bucketId=?");
      p.setInt(1, bucketId);
      p.executeUpdate();

      p = connectionLocal.get().prepareStatement("DELETE FROM buckets WHERE bucketId=?");
      p.setInt(1, bucketId);
      return p.executeUpdate();
    } finally {
      closeDbStuff(null, p);
    }
  }

  /**
   * Insert a row into Audit table
   *
   * @param audit Contains the audit information
   * @return TRUE if the audit was inserted and FALSE in otherwise
   * @throws SQLException
   */
  public boolean insertAudit(Audit audit) throws SQLException {

    PreparedStatement p = null;

    try {
      p = connectionLocal.get().prepareStatement("INSERT INTO audit (bucketName, keyValue, operation, uuid) VALUES (?,?,?,?)");

      p.setString(1, audit.getBucket());
      //The key could be NULL if the operation is about bucket
      p.setString(2, audit.getKey() == null ? "" : audit.getKey());
      p.setString(3, audit.getOperation().getValue());
      //The versionChecksum could be NULL if the operation is about bucket
      p.setString(4, audit.getUuid() == null ? "" : audit.getUuid().toString());

      return p.executeUpdate() > 0;
    } finally {
      closeDbStuff(null, p);
    }
  }

  /**
   * List the audit table
   *
   * @param bucket    a single String representing the bucket name
   * @param key       a single String identifying the object key
   * @param operation represents the operation type
   * @param timestamp represents the creation date time of audit row.
   * @return {@link org.plos.repo.models.Audit List}
   * @throws SQLException
   */
  public List<Audit> listAudit(String bucket, String key, String uuid, Operation operation, Timestamp timestamp) throws SQLException, RepoException {

    List<Audit> repoAudit = new ArrayList<>();
    PreparedStatement p = null;
    ResultSet result = null;
    boolean filter = false;

    try {
      StringBuilder query = new StringBuilder("SELECT * FROM audit ");

      if (bucket != null) {
        filter = true;
        query.append(" WHERE bucketName = ? ");
      }
      if (key != null) {
        filter = true;
        query.append((filter ? " AND " : " WHERE "));
        query.append(" keyValue = ? ");
      }
      if (operation != null) {
        filter = true;
        query.append((filter ? " AND " : " WHERE "));
        query.append(" operation = ? ");
      }
      if (uuid != null) {
        filter = true;
        query.append((filter ? " AND " : " WHERE "));
        query.append(" uuid = ? ");
      }
      if (timestamp != null) {
        query.append((filter ? " AND " : " WHERE "));
        query.append(" timestamp >= ? ");
      }

      p = connectionLocal.get().prepareStatement(query.toString());

      int i = 1;

      if (bucket != null) {
        p.setString(i++, bucket);
      }
      if (key != null) {
        p.setString(i++, key);
      }
      if (operation != null) {
        p.setString(i++, operation.getValue());
      }
      if (uuid != null) {
        p.setString(i++, uuid);
      }
      if (timestamp != null) {
        p.setTimestamp(i++, timestamp);
      }

      result = p.executeQuery();

      while (result.next()) {
        repoAudit.add(mapAuditRow(result));
      }

      return repoAudit;
    } finally {
      closeDbStuff(result, p);
    }
  }



  /**
   * Returns a list of audit records order by creation date. The resulting list will be paginated using
   * <code>offset</code> and <code>limit</code> parameters
   *
   * @param offset         an Integer used to determine the offset of the response
   * @param limit          an Integer used to determine the limit of the response
   * @return a list of {@link org.plos.repo.models.Audit}
   * @throws SQLException
   */
  public List<Audit> listAuditRecords(Integer offset, Integer limit) throws SQLException, RepoException {
    List<Audit> auditRecords = new ArrayList<>();

    PreparedStatement p = null;
    ResultSet result = null;

    try {

      String query = "SELECT * FROM audit a ORDER BY a.timestamp LIMIT " + limit + " OFFSET " + offset;
      p = connectionLocal.get().prepareStatement(query);

      result = p.executeQuery();

      while (result.next()) {
        auditRecords.add(mapAuditRow(result));
      }

      return auditRecords;
    } finally {
      closeDbStuff(result, p);
    }
  }

}
