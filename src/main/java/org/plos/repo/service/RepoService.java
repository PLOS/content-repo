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

import com.google.common.util.concurrent.Striped;
import org.apache.commons.lang.StringUtils;
import org.plos.repo.models.Bucket;
import org.plos.repo.models.Object;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLEncoder;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * This service handles all communication to the objectstore and sqlservice
 */
public class RepoService {

  private static final Logger log = LoggerFactory.getLogger(RepoService.class);

  private Striped<ReadWriteLock> rwLocks = Striped.lazyWeakReadWriteLock(32);

  @Inject
  private ObjectStore objectStore;

  @Inject
  private SqlService sqlService;


  public enum CreateMethod {
    NEW, VERSION, AUTO
  }


  private void sqlReleaseConnection() throws RepoException {

    try {
      sqlService.releaseConnection();
    } catch (SQLException e) {
      throw new RepoException(RepoException.Type.ServerError, e);
    }

  }

  private void sqlRollback(String data) throws RepoException {

    log.error("DB rollback: " + data + "\n" +
        StringUtils.join(Thread.currentThread().getStackTrace(), "\n\t"));

    try {
      sqlService.transactionRollback();
    } catch (SQLException e) {
      throw new RepoException(RepoException.Type.ServerError, e);
    }
  }

  public List<Bucket> listBuckets() throws RepoException {
    try {
      sqlService.getConnection();
      return sqlService.listBuckets();

    } catch (SQLException e) {
      throw new RepoException(RepoException.Type.ServerError, e);
    } finally {
      sqlReleaseConnection();
    }
  }

  public void createBucket(String name) throws RepoException {

    Lock writeLock = this.rwLocks.get(name).writeLock();
    writeLock.lock();

    boolean rollback = false;

    Bucket bucket = new Bucket(name);

    try {

      sqlService.getConnection();

      if (sqlService.getBucketId(name) != null)
        throw new RepoException(RepoException.Type.ClientError, "Bucket already exists in database: " + name);

      if (objectStore.bucketExists(bucket))
        throw new RepoException(RepoException.Type.ClientError, "Bucket already exists in object store: " + name);

      if (!ObjectStore.isValidFileName(name))
        throw new RepoException(RepoException.Type.ClientError, "Unable to create bucket. Name contains illegal characters: " + name);

      rollback = true;

      if (!objectStore.createBucket(bucket))
        throw new RepoException(RepoException.Type.ClientError, "Unable to create bucket in object store: " + name);


      if (!sqlService.insertBucket(bucket)) {
        throw new RepoException(RepoException.Type.ClientError, "Unable to create bucket in database: " + name);
      }

      sqlService.transactionCommit();

      rollback = false;

    } catch (SQLException e) {
      throw new RepoException(RepoException.Type.ServerError, e);
    } finally {

      if (rollback) {
        sqlRollback("bucket " + name);
        objectStore.deleteBucket(bucket);
        // TODO: check to make sure objectStore.deleteBucket didnt fail
      }

      sqlReleaseConnection();
      writeLock.unlock();

    }

  }

  public void deleteBucket(String name) throws RepoException {

    Lock writeLock = this.rwLocks.get(name).writeLock();
    writeLock.lock();

    boolean rollback = false;

    Bucket bucket = new Bucket(name);

    try {
      sqlService.getConnection();

      if (sqlService.getBucketId(name) == null)
        throw new RepoException(RepoException.Type.ItemNotFound, "Bucket not found in database: " + name);

      if (!objectStore.bucketExists(bucket))
        throw new RepoException(RepoException.Type.ItemNotFound, "Bucket not found in object store: " + name);

      if (sqlService.listObjects(name, 0, 1, true).size() != 0)
        throw new RepoException(RepoException.Type.ClientError, "Cannot delete bucket because it contains objects: " + name );

      rollback = true;

      if (!objectStore.deleteBucket(bucket))
        throw new RepoException(RepoException.Type.ServerError, "Unable to delete bucket in object store: " + name);

      if (sqlService.deleteBucket(name) == 0)
        throw new RepoException(RepoException.Type.ServerError, "Unable to delete bucket in database: " + name);

      sqlService.transactionCommit();
      rollback = false;

    } catch (SQLException e) {
      throw new RepoException(RepoException.Type.ServerError, e);
    } finally {

      if (rollback) {
        sqlRollback("bucket " + name);

        objectStore.createBucket(bucket);
        // TODO: validate objectStore.createBucket return values

      }

      sqlReleaseConnection();
      writeLock.unlock();
    }

  }

  public boolean serverSupportsReproxy() {
    return objectStore.hasXReproxy();
  }

  public List<Object> listObjects(String bucketName, Integer offset, Integer limit, boolean includeDeleted) throws RepoException {

    // TODO: should this function return a list of objects and their nested versions instead of one flat last?

    try {

      sqlService.getConnection();

      if (bucketName != null && sqlService.getBucketId(bucketName) == null)
        throw new RepoException(RepoException.Type.ItemNotFound, "Bucket not found");

      return sqlService.listObjects(bucketName, offset, limit, includeDeleted);
    } catch (SQLException e) {
      throw new RepoException(RepoException.Type.ServerError, e);
    } finally {
      sqlReleaseConnection();
    }
  }

  public Object getObject(String bucketName, String key, Integer version) throws RepoException {

    Lock readLock = this.rwLocks.get(bucketName + key).readLock();
    readLock.lock();

    Object object;

    try {
      sqlService.getConnection();

      if (version == null)
        object = sqlService.getObject(bucketName, key);
      else
        object = sqlService.getObject(bucketName, key, version);

      if (object == null)
        throw new RepoException(RepoException.Type.ItemNotFound, "Object not found");

      return object;

    } catch (SQLException e) {
      throw new RepoException(RepoException.Type.ServerError, e);
    } finally {
      sqlReleaseConnection();
      readLock.unlock();
    }

  }

  public List<Object> getObjectVersions(Object object) throws RepoException {

    Lock readLock = this.rwLocks.get(object.bucketName + object.key).readLock();
    readLock.lock();

    try {
      sqlService.getConnection();
      return sqlService.listObjectVersions(object);
    } catch (SQLException e) {
      throw new RepoException(RepoException.Type.ServerError, e);
    } finally {
      sqlReleaseConnection();
      readLock.unlock();
    }
  }

  public URL[] getObjectReproxy(Object object) throws RepoException {
    try {
      sqlService.getConnection();
      return objectStore.getRedirectURLs(object);
    } catch (Exception e) {
      throw new RepoException(RepoException.Type.ServerError, e);
    } finally {
      sqlReleaseConnection();
    }
  }

  public String getObjectContentType(Object object) {
    String contentType = object.contentType;

    if (object.contentType == null || object.contentType.isEmpty())
      contentType = MediaType.APPLICATION_OCTET_STREAM;

    return contentType;
  }

  public String getObjectExportFileName(Object object) throws RepoException {

    String exportFileName = object.key;

    if (object.downloadName != null)
      exportFileName = object.downloadName;

    try {
      return URLEncoder.encode(exportFileName, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new RepoException(RepoException.Type.ServerError, e);
    }

  }

  public InputStream getObjectInputStream(Object object) throws RepoException {
    try {
      return objectStore.getInputStream(object);
    } catch (Exception e) {
      throw new RepoException(RepoException.Type.ServerError, e);
    }
  }

  public void deleteObject(String bucketName, String key, Integer version) throws RepoException {

    Lock writeLock = this.rwLocks.get(bucketName + key).writeLock();
    writeLock.lock();

    boolean rollback = true;

    try {

      sqlService.getConnection();

      if (sqlService.markObjectDeleted(key, bucketName, version) == 0)
        throw new RepoException(RepoException.Type.ItemNotFound, "Object not found");

      sqlService.transactionCommit();
      rollback = false;

    } catch (SQLException e) {
      throw new RepoException(RepoException.Type.ServerError, e);
    } finally {

      if (rollback) {
        sqlRollback("object " + bucketName + ", " + key + ", " + version);
      }

      sqlReleaseConnection();
      writeLock.unlock();
    }
  }

  public Object createObject(CreateMethod method,
                             String key,
                             String bucketName,
                             String contentType,
                             String downloadName,
                             Timestamp timestamp,
                             InputStream uploadedInputStream) throws RepoException {


    Lock writeLock = this.rwLocks.get(bucketName + key).writeLock();
    writeLock.lock();

    Object existingObject;

    try {

      try {
        existingObject = getObject(bucketName, key, null);
      } catch (RepoException e) {
        if (e.getType() == RepoException.Type.ItemNotFound)
          existingObject = null;
        else
          throw e;
      }

      switch (method) {

        case NEW:
          if (existingObject != null)
            throw new RepoException(RepoException.Type.ClientError, "Attempting to create an object with a key that already exists.");
          return createNewObject(key, bucketName, contentType, downloadName, timestamp, uploadedInputStream);

        case VERSION:
          return updateObject(bucketName, contentType, downloadName, timestamp, uploadedInputStream, existingObject);

        case AUTO:
          if (existingObject == null)
            return createNewObject(key, bucketName, contentType, downloadName, timestamp, uploadedInputStream);
          else
            return updateObject(bucketName, contentType, downloadName, timestamp, uploadedInputStream, existingObject);

        default:
          throw new RepoException(RepoException.Type.ClientError, "Invalid creation method: " + method.toString());
      }
    } finally {
      writeLock.unlock();
    }

  }

  private Object createNewObject(String key,
                                String bucketName,
                                String contentType,
                                String downloadName,
                                Timestamp timestamp,
                                InputStream uploadedInputStream) throws RepoException {

    if (uploadedInputStream == null)
      throw new RepoException(RepoException.Type.ClientError, "No data specified");

    ObjectStore.UploadInfo uploadInfo = null;
    Integer bucketId, versionNumber;

    Object object;

    boolean rollback = false;

    try {

      try {
        sqlService.getConnection();
        bucketId = sqlService.getBucketId(bucketName);
      } catch (SQLException e) {
        throw new RepoException(RepoException.Type.ServerError, e);
      }

      if (bucketId == null)
        throw new RepoException(RepoException.Type.ClientError, "Can not find bucket " + bucketName);

      uploadInfo = objectStore.uploadTempObject(uploadedInputStream);

      try {
        uploadedInputStream.close();
      } catch (IOException e) {
        throw new RepoException(RepoException.Type.ServerError, e);
      }

      if (uploadInfo.getSize() == 0) {
        throw new RepoException(RepoException.Type.ClientError, "Uploaded data must be non-empty");
      }

      try {
        versionNumber = sqlService.getNextAvailableVersionNumber(bucketName, key);
      } catch (SQLException e) {
        throw new RepoException(RepoException.Type.ServerError, e);
      }

      object = new Object(null, key, uploadInfo.getChecksum(), timestamp, downloadName, contentType, uploadInfo.getSize(), null, bucketId, bucketName, versionNumber, Object.Status.USED);

      rollback = true;

      // determine if the object should be added to the store or not
      if (objectStore.objectExists(object)) {

//      if (FileUtils.contentEquals(tempFile, new File(objectStore.getObjectLocationString(bucketName, checksum)))) {
//        log.info("not adding object to store since content exists");
//      } else {
//        log.info("checksum collision!!");
//        status = HttpStatus.CONFLICT;
//      }

        // dont bother storing the file since the data already exists in the system

      } else {
        if (!objectStore.saveUploadedObject(new Bucket(bucketName), uploadInfo, object)) {
          throw new RepoException(RepoException.Type.ServerError, "Error saving content to object store");
        }
      }

      // add a record to the DB

      if (sqlService.insertObject(object) == 0) {
        throw new RepoException(RepoException.Type.ServerError, "Error saving content to database");
      }

      sqlService.transactionCommit();
      rollback = false;

    } catch (SQLException e) {
      throw new RepoException(RepoException.Type.ServerError, e);
    } finally {

      if (uploadInfo != null)
        objectStore.deleteTempUpload(uploadInfo);

      if (rollback) {
        sqlRollback("object " + bucketName + ", " + key);
        // TODO: handle objectStore rollback, or not?
      }

      sqlReleaseConnection();
    }

    return object;
  }

  private Object updateObject(String bucketName,
                          String contentType,
                          String downloadName,
                          Timestamp timestamp,
                          InputStream uploadedInputStream,
                          Object object) throws RepoException {


    if (object == null)
      throw new RepoException(RepoException.Type.ClientError, "Cannot create a new version of an existing object");

    ObjectStore.UploadInfo uploadInfo = null;
    boolean rollback = false;

    try {
      sqlService.getConnection();
      Integer bucketId = sqlService.getBucketId(bucketName);

      if (bucketId == null)
        throw new RepoException(RepoException.Type.ClientError, "Can not find bucket: " + bucketName);

      // copy over values from previous object, if they are not specified in the request
      if (contentType != null)
        object.contentType = contentType;

      if (downloadName != null)
        object.downloadName = downloadName;

      object.timestamp = timestamp;
      object.versionNumber = sqlService.getNextAvailableVersionNumber(bucketName, object.key);
      object.id = null;  // remove this since it refers to the old object

      uploadInfo = objectStore.uploadTempObject(uploadedInputStream);
      uploadedInputStream.close();

      rollback = true;

      if (uploadInfo.getSize() == 0) {
        // handle metadata-only update
      } else {

        // determine if the object should be added to the store or not
        object.checksum = uploadInfo.getChecksum();
        object.size = uploadInfo.getSize();

        if (!objectStore.objectExists(object)) {
          if (!objectStore.saveUploadedObject(new Bucket(bucketName), uploadInfo, object)) {
            throw new RepoException(RepoException.Type.ServerError, "Error saving content to object store");
          }
        }
      }

      // add a record to the DB

      if (sqlService.insertObject(object) == 0) {
        throw new RepoException(RepoException.Type.ServerError, "Error saving content to database");
      }

      sqlService.transactionCommit();
      rollback = false;

    } catch (SQLException | IOException e) {
      throw new RepoException(RepoException.Type.ServerError, e);
    } finally {

      if (uploadInfo != null)
        objectStore.deleteTempUpload(uploadInfo);

      if (rollback) {
        sqlRollback("object " + bucketName + ", " + object.key);
        // TODO: handle objectStore rollback, or not?
      }

      sqlReleaseConnection();
    }

    return object;
  }

}
