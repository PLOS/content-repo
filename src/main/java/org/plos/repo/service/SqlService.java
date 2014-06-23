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

  public Integer getNextAvailableVersionNumber(String bucketName, String key) throws SQLException {

    PreparedStatement p = null;
    ResultSet result = null;

    try {

      p = connectionLocal.get().prepareStatement("SELECT versionNumber FROM objects a, buckets b WHERE a.bucketId = b.bucketId AND b.bucketName=? AND objKey=? ORDER BY versionNumber DESC LIMIT 1");

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

}
