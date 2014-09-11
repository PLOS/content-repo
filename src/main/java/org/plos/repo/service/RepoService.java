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
import org.plos.repo.models.*;
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

  // default page size = number of objects returned when no limit= parameter supplied.
  private static final Integer DEFAULT_PAGE_SIZE = 1000;

  // maximum allowed value of page size, i.e., limit= parameter
  private static final Integer MAX_PAGE_SIZE = 10000;


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
      throw new RepoException(e);
    }

  }

  private void sqlRollback(String data) throws RepoException {

    log.error("DB rollback: " + data + "\n" +
        StringUtils.join(Thread.currentThread().getStackTrace(), "\n\t"));

    try {
      sqlService.transactionRollback();
    } catch (SQLException e) {
      throw new RepoException(e);
    }
  }

  public List<Bucket> listBuckets() throws RepoException {
    try {
      sqlService.getConnection();
      return sqlService.listBuckets();

    } catch (SQLException e) {
      throw new RepoException(e);
    } finally {
      sqlReleaseConnection();
    }
  }

  public Bucket createBucket(String name) throws RepoException {

    Lock writeLock = this.rwLocks.get(name).writeLock();
    writeLock.lock();

    boolean rollback = false;

    Bucket bucket = new Bucket(name);

    try {

      if (!ObjectStore.isValidFileName(name))
        throw new RepoException(RepoException.Type.IllegalBucketName);

      sqlService.getConnection();

      if (sqlService.getBucket(name) != null)
        throw new RepoException(RepoException.Type.BucketAlreadyExists);

      if (Boolean.TRUE.equals(objectStore.bucketExists(bucket)))
        throw new RepoException("Bucket exists in object store but not in database: " + name);

      rollback = true;

      if (Boolean.FALSE.equals(objectStore.createBucket(bucket)))
        throw new RepoException("Unable to create bucket in object store: " + name);

      if (!sqlService.insertBucket(bucket)) {
        throw new RepoException("Unable to create bucket in database: " + name);
      }

      sqlService.transactionCommit();

      rollback = false;

      return sqlService.getBucket(name);

    } catch (SQLException e) {
      throw new RepoException(e);
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

      if (sqlService.getBucket(name) == null)
        throw new RepoException(RepoException.Type.BucketNotFound);

      if (Boolean.FALSE.equals(objectStore.bucketExists(bucket)))
        throw new RepoException("Bucket exists in database but not in object store: " + name);

      if (sqlService.listObjects(name, 0, 1, true).size() != 0)
        throw new RepoException(RepoException.Type.CantDeleteNonEmptyBucket);

      rollback = true;

      if (Boolean.FALSE.equals(objectStore.deleteBucket(bucket)))
        throw new RepoException("Unable to delete bucket in object store: " + name);

      if (sqlService.deleteBucket(name) == 0)
        throw new RepoException("Unable to delete bucket in database: " + name);

      sqlService.transactionCommit();
      rollback = false;

    } catch (SQLException e) {
      throw new RepoException(e);
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

    if (offset == null)
      offset = 0;
    if (limit == null)
      limit = DEFAULT_PAGE_SIZE;

    try {

      if (offset < 0)
        throw new RepoException(RepoException.Type.InvalidOffset);

      if (limit <= 0 || limit > MAX_PAGE_SIZE)
        throw new RepoException(RepoException.Type.InvalidLimit);

      sqlService.getConnection();

      if (bucketName != null && sqlService.getBucket(bucketName) == null)
        throw new RepoException(RepoException.Type.BucketNotFound);

      return sqlService.listObjects(bucketName, offset, limit, includeDeleted);
    } catch (SQLException e) {
      throw new RepoException(e);
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
        throw new RepoException(RepoException.Type.ObjectNotFound);

      return object;

    } catch (SQLException e) {
      throw new RepoException(e);
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
      throw new RepoException(e);
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
      throw new RepoException(e);
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
      throw new RepoException(e);
    }

  }

  public InputStream getObjectInputStream(Object object) throws RepoException {
    try {
      return objectStore.getInputStream(object);
    } catch (Exception e) {
      throw new RepoException(e);
    }
  }

  public void deleteObject(String bucketName, String key, Integer version) throws RepoException {

    Lock writeLock = this.rwLocks.get(bucketName + key).writeLock();
    writeLock.lock();

    boolean rollback = false;

    try {

      sqlService.getConnection();

      if (key == null)
        throw new RepoException(RepoException.Type.NoKeyEntered);

      if (version == null)
        throw new RepoException(RepoException.Type.NoVersionEntered);

      rollback = true;

      if (sqlService.markObjectDeleted(key, bucketName, version) == 0)
        throw new RepoException(RepoException.Type.ObjectNotFound);

      sqlService.transactionCommit();
      rollback = false;

    } catch (SQLException e) {
      throw new RepoException(e);
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

      if (key == null)
        throw new RepoException(RepoException.Type.NoKeyEntered);

      if (bucketName == null)
        throw new RepoException(RepoException.Type.NoBucketEntered);

      try {
        existingObject = getObject(bucketName, key, null);
      } catch (RepoException e) {
        if (e.getType() == RepoException.Type.ObjectNotFound)
          existingObject = null;
        else
          throw e;
      }

      switch (method) {

        case NEW:
          if (existingObject != null)
            throw new RepoException(RepoException.Type.CantCreateNewObjectWithUsedKey);
          return createNewObject(key, bucketName, contentType, downloadName, timestamp, uploadedInputStream);

        case VERSION:
          if (existingObject == null)
            throw new RepoException(RepoException.Type.CantCreateVersionWithNoOrig);
          return updateObject(bucketName, contentType, downloadName, timestamp, uploadedInputStream, existingObject);

        case AUTO:
          if (existingObject == null)
            return createNewObject(key, bucketName, contentType, downloadName, timestamp, uploadedInputStream);
          else
            return updateObject(bucketName, contentType, downloadName, timestamp, uploadedInputStream, existingObject);

        default:
          throw new RepoException(RepoException.Type.InvalidCreationMethod);
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

    ObjectStore.UploadInfo uploadInfo = null;
    Integer versionNumber;
    Bucket bucket;

    Object object;

    boolean rollback = false;

    try {

      try {
        sqlService.getConnection();
        bucket = sqlService.getBucket(bucketName);
      } catch (SQLException e) {
        throw new RepoException(e);
      }

      if (bucket == null)
        throw new RepoException(RepoException.Type.BucketNotFound);

      uploadInfo = objectStore.uploadTempObject(uploadedInputStream);

      try {
        uploadedInputStream.close();
      } catch (IOException e) {
        throw new RepoException(e);
      }

      if (uploadInfo.getSize() == 0) {
        throw new RepoException(RepoException.Type.ObjectDataEmpty);
      }

      try {
        versionNumber = sqlService.getNextAvailableVersionNumber(bucketName, key);
      } catch (SQLException e) {
        throw new RepoException(e);
      }

      object = new Object(null, key, uploadInfo.getChecksum(), timestamp, downloadName, contentType, uploadInfo.getSize(), null, bucket.bucketId, bucketName, versionNumber, Object.Status.USED);

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
        if (Boolean.FALSE.equals(objectStore.saveUploadedObject(new Bucket(bucketName), uploadInfo, object))) {
          throw new RepoException("Error saving content to object store");
        }
      }

      // add a record to the DB

      if (sqlService.insertObject(object) == 0) {
        throw new RepoException("Error saving content to database");
      }

      sqlService.transactionCommit();
      rollback = false;

    } catch (SQLException e) {
      throw new RepoException(e);
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

    ObjectStore.UploadInfo uploadInfo = null;
    boolean rollback = false;

    Object newObject = null;

    try {

      uploadInfo = objectStore.uploadTempObject(uploadedInputStream);
      uploadedInputStream.close();

      String checksum = null;
      Long size = null;
      if (uploadInfo.getSize() == 0) {
        // handle metadata-only update
      } else {

        // determine if the object should be added to the store or not
        checksum = uploadInfo.getChecksum();
        size = uploadInfo.getSize();

        if (Boolean.FALSE.equals(objectStore.objectExists(object))) {
          if (Boolean.FALSE.equals(objectStore.saveUploadedObject(new Bucket(bucketName), uploadInfo, object))) {
            throw new RepoException("Error saving content to object store");
          }
        }
      }

      newObject = new Object(null, object.key, checksum, null, downloadName, contentType, size, null, object.bucketId, bucketName, null, Object.Status.USED);

      if (object.areSimilar(newObject)){
        return object;
      }

      rollback = true;

      sqlService.getConnection();

      if (sqlService.getBucket(bucketName) == null)
        throw new RepoException(RepoException.Type.BucketNotFound);

      // add a record to the DB
      newObject.versionNumber = sqlService.getNextAvailableVersionNumber(bucketName, object.key);

      if (sqlService.insertObject(newObject) == 0) {
        throw new RepoException("Error saving content to database");
      }

      sqlService.transactionCommit();
      rollback = false;

    } catch (SQLException | IOException e) {
      throw new RepoException(e);
    } finally {

      if (uploadInfo != null)
        objectStore.deleteTempUpload(uploadInfo);

      if (rollback) {
        sqlRollback("object " + bucketName + ", " + object.key);
        // TODO: handle objectStore rollback, or not?
      }

      sqlReleaseConnection();

    }

    return newObject;
  }

  /**
   * Returns a list of collections for the given bucket name <code>bucketName</code>. In case pagination
   * parameters <code>offset</code> and <code>limit</code> are not present, it loads the default pagination data.
   * @param bucketName a single String representing the bucket name in where to look the collection
   * @param offset an Integer used to determine the offset of the response
   * @param limit an Integer used to determine the limit of the response
   * @return a list of {@link org.plos.repo.models.Collection}
   * @throws RepoException
   */
  public List<Collection> listCollections(String bucketName, Integer offset, Integer limit) throws RepoException {

    if (offset == null)
      offset = 0;
    if (limit == null)
      limit = DEFAULT_PAGE_SIZE;

    try {

      validatePagination(offset, limit);

      sqlService.getConnection();

      validateBucketData(bucketName, sqlService);

      return sqlService.listCollections(bucketName, offset, limit);

    } catch (SQLException e) {
      throw new RepoException(e);
    } finally {
      sqlReleaseConnection();
    }

  }

  /**
   * Returns a collection identiied by <code>bucketName</code> and <code>key</code>. If <code>version</code> is null, it returns the latest
   * version available, if it is not, it returns the requested version.
   * @param bucketName a single String representing the bucket name in where to look the collection
   * @param key key a single String identifying the collection key
   * @param version an int value representing the version number of the collection
   * @return a collection {@link org.plos.repo.models.Collection} or null is the desired collection does not exists
   * @throws RepoException
   */
  public Collection getCollection(String bucketName, String key, Integer version) throws RepoException {

    Lock readLock = this.rwLocks.get(bucketName + key).readLock();
    readLock.lock();

    Collection collection;

    try {
      sqlService.getConnection();

      if (key == null)
        throw new RepoException(RepoException.Type.NoCollectionKeyEntered);

      if (version == null)
        collection = sqlService.getCollection(bucketName, key);
      else
        collection = sqlService.getCollection(bucketName, key, version);

      if (collection == null)
        throw new RepoException(RepoException.Type.CollectionNotFound);

      collection.setVersions(this.getCollectionVersions(collection));

      return collection;

    } catch (SQLException e) {
      throw new RepoException(e);
    } finally {
      sqlReleaseConnection();
      readLock.unlock();
    }

  }

  /**
   * Returns a list of all collection versions for the given <code>collection</code>
   * @param collection a single {@link org.plos.repo.models.Collection}
   * @return a list of {@link org.plos.repo.models.Collection}
   * @throws RepoException
   */
  public List<Collection> getCollectionVersions(Collection collection) throws RepoException {

    Lock readLock = this.rwLocks.get(collection.getBucketName() + collection.getKey()).readLock();
    readLock.lock();

    try {
      sqlService.getConnection();
      return sqlService.listCollectionVersions(collection);
    } catch (SQLException e) {
      throw new RepoException(e);
    } finally {
      sqlReleaseConnection();
      readLock.unlock();
    }
  }

  /**
   * Deletes the collection define by <code>bucketName</code> , <code>key</code> , <code>version</code>
   * @param bucketName a single String identifying the bucket name where the collection is.
   * @param key a single String identifying the collection key
   * @param version an int value representing the version number of the collection
   * @throws RepoException
   */
  public void deleteCollection(String bucketName, String key, Integer version) throws RepoException {

    Lock writeLock = this.rwLocks.get(bucketName + key).writeLock();
    writeLock.lock();

    boolean rollback = false;

    try {

      sqlService.getConnection();

      if (key == null)
        throw new RepoException(RepoException.Type.NoCollectionKeyEntered);

      if (version == null)
        throw new RepoException(RepoException.Type.NoCollectionVersionEntered);

      rollback = true;

      if (sqlService.markCollectionDeleted(key, bucketName, version) == 0)
        throw new RepoException(RepoException.Type.CollectionNotFound);

      sqlService.transactionCommit();
      rollback = false;

    } catch (SQLException e) {
      throw new RepoException(e);
    } finally {

      if (rollback) {
        sqlRollback("object " + bucketName + ", " + key + ", " + version);
      }

      sqlReleaseConnection();
      writeLock.unlock();
    }
  }

  protected void validatePagination(Integer offset, Integer limit) throws RepoException {
    if (offset < 0)
      throw new RepoException(RepoException.Type.InvalidOffset);

    if (limit <= 0 || limit > MAX_PAGE_SIZE)
      throw new RepoException(RepoException.Type.InvalidLimit);
  }

  protected void validateBucketData(String bucketName, SqlService sqlService) throws RepoException, SQLException{
    if (bucketName != null && sqlService.getBucket(bucketName) == null)
      throw new RepoException(RepoException.Type.BucketNotFound);
  }

  public Collection createCollection(CreateMethod method, SmallCollection smallCollection) throws RepoException {

    String key = smallCollection.getKey();
    String bucketName = smallCollection.getBucketName();
    Timestamp timestamp = smallCollection.getTimestamp();
    List<SmallObject> smallObjects = smallCollection.getObjects();

    Lock writeLock = this.rwLocks.get(bucketName + key).writeLock();
    writeLock.lock();

    Collection existingCollection;

    try {

      if (key == null)
        throw new RepoException(RepoException.Type.NoKeyEntered);

      if (bucketName == null)
        throw new RepoException(RepoException.Type.NoBucketEntered);

      try {
        existingCollection = getCollection(bucketName, key, null);
      } catch (RepoException e) {
        if (e.getType() == RepoException.Type.CollectionNotFound)
          existingCollection = null;
        else
          throw e;
      }

      verifyObjects(smallObjects);

      switch (method) {

        case NEW:
          if (existingCollection != null)
            throw new RepoException(RepoException.Type.CantCreateNewCollectionWithUsedKey);
          return createNewCollection(key, bucketName, timestamp, smallObjects);

        case VERSION:
          if (existingCollection == null)
            throw new RepoException(RepoException.Type.CantCreateCollectionVersionWithNoOrig);
          return updateCollection(key, bucketName, timestamp, existingCollection, smallObjects);

        case AUTO:
          if (existingCollection == null)
            return createNewCollection(key, bucketName, timestamp, smallCollection.getObjects());
          else
            return updateCollection(key, bucketName, timestamp, existingCollection, smallObjects);

        default:
          throw new RepoException(RepoException.Type.InvalidCreationMethod);
      }
    } finally {
      writeLock.unlock();
    }

  }

  private void verifyObjects(List<SmallObject> objects) throws RepoException {

    if (objects == null || objects.size() == 0 ){
      throw new RepoException(RepoException.Type.CantCreateCollectionWithNoObjects);
    }
  }

  private Collection createNewCollection(String key,
                                         String bucketName,
                                         Timestamp timestamp,
                                         List<SmallObject> objects) throws RepoException {

    Bucket bucket = null;

    try {
      sqlService.getConnection();
      bucket = sqlService.getBucket(bucketName);
    } catch (SQLException e) {
      throw new RepoException(e);
    }

    if (bucket == null)
      throw new RepoException(RepoException.Type.BucketNotFound);

    return createCollection(key, bucketName, timestamp, bucket.bucketId, objects);
  }

  private Collection updateCollection(String key,
                                      String bucketName,
                                      Timestamp timestamp,
                                      Collection existingCollection,
                                      List<SmallObject> objects) throws RepoException {

    if (areCollectionsSimilar(key, bucketName, objects, existingCollection)){
      return existingCollection;
    }

    return createCollection(key, bucketName, timestamp, existingCollection.getBucketId(), objects);

  }

  private Boolean areCollectionsSimilar(String key,
                                        String bucketName,
                                        List<SmallObject> objects,
                                        Collection existingCollection){

    Boolean similar = existingCollection.getKey().equals(key) &&
        existingCollection.getKey().equals(bucketName) &&
        existingCollection.getStatus().equals(Collection.Status.USED) &&
        objects.size() == existingCollection.getObjects().size();

    int i = 0;

    for ( ; i <  objects.size() & similar ; i++){

      SmallObject smallObject = objects.get(i);

      int y = 0;
      for( ; y < existingCollection.getObjects().size() & similar; y++ ){
        Object object = existingCollection.getObjects().get(y);
        if (object.key.equals(smallObject.getKey()) &&
            object.bucketName.equals(smallObject.getBucketName()) &&
            object.versionNumber.equals(smallObject.getVersionNumber())){
          break;

        }
      }

      if ( y == existingCollection.getObjects().size()){
        similar = false;
      }
    }


    return similar;

  }

  private Collection createCollection(String key,
                                      String bucketName,
                                      Timestamp timestamp,
                                      Integer bucketId,
                                      List<SmallObject> smallObjects) throws RepoException {

    Integer versionNumber;
    Collection collection;
    boolean rollback = false;

    try {

      try {
        versionNumber = sqlService.getNextAvailableVersionNumber(bucketName, key);   // change to support collections
      } catch (SQLException e) {
        throw new RepoException(e);
      }

      collection = new Collection(null, key, timestamp, bucketId, bucketName, versionNumber, Collection.Status.USED);

      rollback = true;

      // add a record to the DB
      Integer collId = sqlService.insertCollection(collection, smallObjects);
      if (collId == -1) {
        throw new RepoException("Error saving content to database");
      }

      for (SmallObject smallObject : smallObjects){

        if (sqlService.insertCollectionObjects(collId, smallObject.getKey(), smallObject.getBucketName(), smallObject.getVersionNumber()) == 0){
          throw new RepoException(RepoException.Type.ObjectCollectionNotFound);
        }

      }

      sqlService.transactionCommit();
      rollback = false;

    } catch (SQLException e) {
      throw new RepoException(e);
    } finally {

      if (rollback) {
        sqlRollback("collection " + bucketName + ", " + key);
      }
      sqlReleaseConnection();
    }

    return collection;

  }

}
