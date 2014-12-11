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
import org.hsqldb.lib.StringUtil;
import org.plos.repo.models.Bucket;
import org.plos.repo.models.RepoObject;
import org.plos.repo.models.Status;
import org.plos.repo.models.input.ElementFilter;
import org.plos.repo.models.validator.TimestampInputValidator;
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
import java.util.Date;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * This service handles all communication to the objectstore and sqlservice for objects and buckets
 */
public class RepoService extends BaseRepoService {

  private Striped<ReadWriteLock> rwLocks = Striped.lazyWeakReadWriteLock(32);

  private static final Logger log = LoggerFactory.getLogger(RepoService.class);

  @Inject
  private ObjectStore objectStore;

  @Inject
  private TimestampInputValidator timestampValidator;

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

  public Bucket createBucket(String name, String creationDateTimeString) throws RepoException {

    timestampValidator.validate(creationDateTimeString, RepoException.Type.CouldNotParseCreationDate);

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

      Timestamp creationDate = creationDateTimeString != null ?
          Timestamp.valueOf(creationDateTimeString) : new Timestamp(new Date().getTime());

      if (!sqlService.insertBucket(bucket, creationDate)) {
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

      if (sqlService.listObjects(name, 0, 1, true, true, null).size() != 0)
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

  public List<RepoObject> listObjects(String bucketName, Integer offset, Integer limit, boolean includeDeleted, boolean includePurged, String tag) throws RepoException {

    // TODO: should this function return a list of objects and their nested versions instead of one flat last?

    if (StringUtil.isEmpty(bucketName)){
      throw new RepoException(RepoException.Type.NoBucketEntered);
    }

    List<RepoObject> repoObjects = null;

    if (offset == null)
      offset = 0;
    if (limit == null)
      limit = DEFAULT_PAGE_SIZE;

    try {

      validatePagination(offset, limit);

      sqlService.getConnection();

      if (bucketName != null && sqlService.getBucket(bucketName) == null)
        throw new RepoException(RepoException.Type.BucketNotFound);

      repoObjects = sqlService.listObjects(bucketName, offset, limit, includeDeleted, includePurged, tag);
    } catch (SQLException e) {
      throw new RepoException(e);
    } finally {
      sqlReleaseConnection();
    }

    return this.addProxyData(repoObjects);

  }

  private List<RepoObject> addProxyData(List<RepoObject> repoObjects) throws RepoException {

    if (repoObjects != null && repoObjects.size() > 0 && this.serverSupportsReproxy()){

      for (RepoObject repoObject : repoObjects){
        repoObject.setReproxyURL(this.getObjectReproxy(repoObject));
      }

    }

    return repoObjects;

  }

  private RepoObject addProxyData(RepoObject repoObject) throws RepoException {

    if (repoObject != null && this.serverSupportsReproxy()){
      repoObject.setReproxyURL(this.getObjectReproxy(repoObject));
    }

    return repoObject;

  }

  public RepoObject getObject(String bucketName, String key, ElementFilter elementFilter) throws RepoException {

    Lock readLock = this.rwLocks.get(bucketName + key).readLock();
    readLock.lock();

    RepoObject repoObject = null;

    try {
      sqlService.getConnection();

      if ((elementFilter == null) || (elementFilter.isEmpty())){
        repoObject = sqlService.getObject(bucketName, key);
      }
      else
        repoObject = sqlService.getObject(bucketName, key, elementFilter.getVersion(), elementFilter.getVersionChecksum(), elementFilter.getTag());

      if (repoObject == null)
        throw new RepoException(RepoException.Type.ObjectNotFound);

    } catch (SQLException e) {
      throw new RepoException(e);
    } finally {
      sqlReleaseConnection();
      readLock.unlock();
    }

    return this.addProxyData(repoObject);

  }

  public List<RepoObject> getObjectVersions(String bucketName, String objectKey) throws RepoException {

    if (objectKey == null)
      throw new RepoException(RepoException.Type.NoKeyEntered);

    Lock readLock = this.rwLocks.get(bucketName + objectKey).readLock();
    readLock.lock();

    List<RepoObject> repoObjects = null ;
    try {
      sqlService.getConnection();
      repoObjects =  sqlService.listObjectVersions(bucketName, objectKey);
    } catch (SQLException e) {
      throw new RepoException(e);
    } finally {
      sqlReleaseConnection();
      readLock.unlock();
    }

    return this.addProxyData(repoObjects);

  }

  public URL[] getObjectReproxy(RepoObject repoObject) throws RepoException {
    try {
      sqlService.getConnection();
      return objectStore.getRedirectURLs(repoObject);
    } catch (Exception e) {
      throw new RepoException(e);
    } finally {
      sqlReleaseConnection();
    }
  }

  public String getObjectContentType(RepoObject repoObject) {
    String contentType = repoObject.getContentType();

    if (repoObject.getContentType() == null || repoObject.getContentType().isEmpty())
      contentType = MediaType.APPLICATION_OCTET_STREAM;

    return contentType;
  }

  public String getObjectExportFileName(RepoObject repoObject) throws RepoException {

    String exportFileName = repoObject.getKey();

    if (repoObject.getDownloadName() != null)
      exportFileName = repoObject.getDownloadName();

    try {
      return URLEncoder.encode(exportFileName, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new RepoException(e);
    }

  }

  public InputStream getObjectInputStream(RepoObject repoObject) throws RepoException {
    try {
      return objectStore.getInputStream(repoObject);
    } catch (Exception e) {
      throw new RepoException(e);
    }
  }

  public void purgeObject(String bucketName, String key, ElementFilter elementFilter) throws RepoException {
    deleteObject(bucketName, key, elementFilter, Status.PURGED);
  }

  public void deleteObject(String bucketName, String key, ElementFilter elementFilter) throws RepoException {
    deleteObject(bucketName, key, elementFilter, Status.DELETED);
  }

  public void deleteObject(String bucketName, String key, ElementFilter elementFilter, Status status ) throws RepoException {

    Lock writeLock = this.rwLocks.get(bucketName + key).writeLock();
    writeLock.lock();

    boolean rollback = false;

    try {

      sqlService.getConnection();

      if (key == null)
        throw new RepoException(RepoException.Type.NoKeyEntered);

      if ((elementFilter == null || elementFilter.isEmpty())) {
        throw new RepoException(RepoException.Type.NoFilterEntered);
      }

      rollback = true;

      if (elementFilter.getTag() != null & elementFilter.getVersionChecksum() == null & elementFilter.getVersion() == null){
        if (sqlService.listObjects(bucketName, 0, 10, false, false, elementFilter.getTag()).size() > 1){
          throw new RepoException(RepoException.Type.MoreThanOneTaggedObject);
        }
      }

      if (sqlService.existsActiveCollectionForObject(key, bucketName,  elementFilter.getVersion(), elementFilter.getVersionChecksum(), elementFilter.getTag())){
        throw new RepoException(RepoException.Type.CantDeleteObjectActiveColl);
      }

      if (Status.DELETED.equals(status)){
        if (sqlService.markObjectDeleted(key, bucketName, elementFilter.getVersion(), elementFilter.getVersionChecksum(), elementFilter.getTag()) == 0)
          throw new RepoException(RepoException.Type.ObjectNotFound);
      }else if (Status.PURGED.equals(status)){
        purgeObjectContentAndDb(key, bucketName, elementFilter);
      }

      sqlService.transactionCommit();
      rollback = false;

    } catch (SQLException e) {
      throw new RepoException(e);
    } finally {

      if (rollback) {
        sqlRollback("object " + bucketName + ", " + key + ", " + elementFilter);
      }

      sqlReleaseConnection();
      writeLock.unlock();
    }
  }

  private void purgeObjectContentAndDb(String key, String bucketName, ElementFilter elementFilter)  throws RepoException {

    try {
      // verify object existence
      RepoObject repoObject = sqlService.getObject(bucketName, key, elementFilter.getVersion(), elementFilter.getVersionChecksum(), elementFilter.getTag(), true, false);
      if (repoObject == null){
        throw new RepoException(RepoException.Type.ObjectNotFound);
      }
      try (InputStream content = getObjectInputStream(repoObject)){
        if (content == null){
          throw new RepoException(RepoException.Type.ObjectNotFound);
        }
      } catch (IOException e) {
        throw new RepoException(e);
      }

      if (sqlService.markObjectPurged(key, bucketName, elementFilter.getVersion(), elementFilter.getVersionChecksum(), elementFilter.getTag()) == 0 ){
        throw new RepoException(RepoException.Type.ObjectNotFound);
      }
      Boolean removed = objectStore.deleteObject(repoObject);
      if (Boolean.FALSE.equals(removed)){
        throw new RepoException(RepoException.Type.ObjectNotFound);
      }
    } catch (SQLException e) {
      throw new RepoException(e);
    }

  }

  public RepoObject createObject(CreateMethod method,
                             String key,
                             String bucketName,
                             String contentType,
                             String downloadName,
                             Timestamp timestamp,
                             InputStream uploadedInputStream,
                             Timestamp creationDateTime,
                             String tag) throws RepoException {


    Lock writeLock = this.rwLocks.get(bucketName + key).writeLock();
    writeLock.lock();

    RepoObject existingRepoObject;

    try {

      if (key == null)
        throw new RepoException(RepoException.Type.NoKeyEntered);

      if (bucketName == null)
        throw new RepoException(RepoException.Type.NoBucketEntered);

      try {
        existingRepoObject = getObject(bucketName, key, null);
      } catch (RepoException e) {
        if (e.getType() == RepoException.Type.ObjectNotFound)
          existingRepoObject = null;
        else
          throw e;
      }

      switch (method) {

        case NEW:
          if (existingRepoObject != null)
            throw new RepoException(RepoException.Type.CantCreateNewObjectWithUsedKey);
          return createNewObject(key, bucketName, contentType, downloadName, timestamp, uploadedInputStream, creationDateTime, tag);

        case VERSION:
          if (existingRepoObject == null)
            throw new RepoException(RepoException.Type.CantCreateVersionWithNoOrig);
          return updateObject(bucketName, contentType, downloadName, timestamp, uploadedInputStream, existingRepoObject, creationDateTime, tag);

        case AUTO:
          if (existingRepoObject == null)
            return createNewObject(key, bucketName, contentType, downloadName, timestamp, uploadedInputStream, creationDateTime, tag);
          else
            return updateObject(bucketName, contentType, downloadName, timestamp, uploadedInputStream, existingRepoObject, creationDateTime, tag);

        default:
          throw new RepoException(RepoException.Type.InvalidCreationMethod);
      }
    } finally {
      writeLock.unlock();
    }

  }

  private RepoObject createNewObject(String key,
                                 String bucketName,
                                 String contentType,
                                 String downloadName,
                                 Timestamp timestamp,
                                 InputStream uploadedInputStream,
                                 Timestamp cretationDateTime,
                                 String tag) throws RepoException {

    ObjectStore.UploadInfo uploadInfo = null;
    Integer versionNumber;
    Bucket bucket;

    RepoObject repoObject;

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
        versionNumber = sqlService.getObjectNextAvailableVersion(bucketName, key);
      } catch (SQLException e) {
        throw new RepoException(e);
      }

      repoObject = new RepoObject(key, bucket.getBucketId(), bucketName, Status.USED);
      repoObject.setChecksum(uploadInfo.getChecksum());
      repoObject.setTimestamp(timestamp);
      repoObject.setDownloadName(downloadName);
      repoObject.setContentType(contentType);
      repoObject.setSize(uploadInfo.getSize());
      repoObject.setTag(tag);
      repoObject.setVersionNumber(versionNumber);
      repoObject.setCreationDate(cretationDateTime);

      repoObject.setVersionChecksum(checksumGenerator.generateVersionChecksum(repoObject));
      rollback = true;

      // determine if the object should be added to the store or not
      if (objectStore.objectExists(repoObject)) {

//      if (FileUtils.contentEquals(tempFile, new File(objectStore.getObjectLocationString(bucketName, checksum)))) {
//        log.info("not adding object to store since content exists");
//      } else {
//        log.info("checksum collision!!");
//        status = HttpStatus.CONFLICT;
//      }

        // dont bother storing the file since the data already exists in the system

      } else {
        if (Boolean.FALSE.equals(objectStore.saveUploadedObject(new Bucket(bucketName), uploadInfo, repoObject))) {
          throw new RepoException("Error saving content to object store");
        }
      }

      // add a record to the DB

      if (sqlService.insertObject(repoObject) == 0) {
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

    return repoObject;
  }

  private RepoObject updateObject(String bucketName,
                              String contentType,
                              String downloadName,
                              Timestamp timestamp,
                              InputStream uploadedInputStream,
                              RepoObject repoObject,
                              Timestamp cretationDateTime,
                              String tag) throws RepoException {

    ObjectStore.UploadInfo uploadInfo = null;
    boolean rollback = false;

    RepoObject newRepoObject = null;

    try {

      // since this method can be used some property of the meta data, the content or both together,
      //if any of the input properties are null, we should populate them with the data of the previous version of the object
      // TODO: if the object is equals to the last version, do we need to update the timestamp?

      newRepoObject = new RepoObject(repoObject.getKey(), repoObject.getBucketId(), bucketName, Status.USED);
      newRepoObject.setTimestamp(timestamp);
      newRepoObject.setDownloadName(downloadName);
      newRepoObject.setContentType(contentType);
      newRepoObject.setTag(tag);
      newRepoObject.setCreationDate(cretationDateTime);
      newRepoObject.setVersionChecksum(checksumGenerator.generateVersionChecksum(newRepoObject));
      sqlService.getConnection();
      if (sqlService.getBucket(bucketName) == null)
        throw new RepoException(RepoException.Type.BucketNotFound);
      // copy over values from previous object, if they are not specified in the request
      if (contentType == null)
        newRepoObject.setContentType(repoObject.getContentType());
      if (downloadName == null)
        newRepoObject.setDownloadName(repoObject.getDownloadName());

      rollback = true;
      if (uploadedInputStream == null) {
      // handle metadata-only update, the content would be the same as the last version of the object
        newRepoObject.setChecksum(repoObject.getChecksum());
      } else {
        // determine if the new object should be added to the store or not
        uploadInfo = objectStore.uploadTempObject(uploadedInputStream);
        uploadedInputStream.close();
        newRepoObject.setChecksum(uploadInfo.getChecksum());
        newRepoObject.setSize(uploadInfo.getSize());
        if (Boolean.FALSE.equals(objectStore.objectExists(newRepoObject))) {
          if (Boolean.FALSE.equals(objectStore.saveUploadedObject(new Bucket(bucketName), uploadInfo, newRepoObject))) {
            throw new RepoException("Error saving content to object store");
          }
        }
      }

      // if the new object and the last version of the object are similar, we just return the last version of the object
      if (repoObject.areSimilar(newRepoObject)){
        rollback = false; // if we are not modifying the DB and reached this point, then the operations were successfully performed
        return repoObject;
      }

      newRepoObject.setVersionNumber(sqlService.getObjectNextAvailableVersion(bucketName, newRepoObject.getKey()));
      // add a record to the DB for the new object
      if (sqlService.insertObject(newRepoObject) == 0) {
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
        sqlRollback("object " + bucketName + ", " + repoObject.getKey());
        // TODO: handle objectStore rollback, or not?
      }

      sqlReleaseConnection();

    }

    return newRepoObject;
  }

   @Override
  public Logger getLog() {
    return log;
  }

}
