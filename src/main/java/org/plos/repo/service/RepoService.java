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
import org.plos.repo.models.input.InputRepoObject;
import org.plos.repo.models.validator.InputRepoObjectValidator;
import org.plos.repo.models.validator.JsonStringValidator;
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
  private InputRepoObjectValidator inputRepoObjectValidator;

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

      if (sqlService.listObjects(name, 0, 1, true, null).size() != 0)
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

  public List<RepoObject> listObjects(String bucketName, Integer offset, Integer limit, boolean includeDeleted, String tag) throws RepoException {

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

      repoObjects = sqlService.listObjects(bucketName, offset, limit, includeDeleted, tag);
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

  public void deleteObject(String bucketName, String key, ElementFilter elementFilter) throws RepoException {

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
        if (sqlService.listObjects(bucketName, 0, 10, false, elementFilter.getTag()).size() > 1){
          throw new RepoException(RepoException.Type.MoreThanOneTaggedObject);
        }
      }

      if (sqlService.existsActiveCollectionForObject(key, bucketName,  elementFilter.getVersion(), elementFilter.getVersionChecksum(), elementFilter.getTag())){
        throw new RepoException(RepoException.Type.CantDeleteObjectActiveColl);
      }

      if (sqlService.markObjectDeleted(key, bucketName, elementFilter.getVersion(), elementFilter.getVersionChecksum(), elementFilter.getTag()) == 0)
        throw new RepoException(RepoException.Type.ObjectNotFound);

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

  public RepoObject createObject(CreateMethod method,
                                 InputRepoObject inputRepoObject) throws RepoException {


    Lock writeLock = this.rwLocks.get(inputRepoObject.getBucketName() + inputRepoObject.getKey()).writeLock();
    writeLock.lock();

    RepoObject existingRepoObject;

    try {

      inputRepoObjectValidator.validate(inputRepoObject);

      try {
        existingRepoObject = getObject(inputRepoObject.getBucketName(), inputRepoObject.getKey(), null);
      } catch (RepoException e) {
        if (e.getType() == RepoException.Type.ObjectNotFound)
          existingRepoObject = null;
        else
          throw e;
      }

      // creates timestamps
      Timestamp creationDate = inputRepoObject.getCreationDateTime() != null ?
          Timestamp.valueOf(inputRepoObject.getCreationDateTime()) : new Timestamp(new Date().getTime());

      Timestamp timestamp = inputRepoObject.getTimestamp() != null ?
          Timestamp.valueOf(inputRepoObject.getTimestamp()) : creationDate;

      switch (method) {

        case NEW:
          if (existingRepoObject != null)
            throw new RepoException(RepoException.Type.CantCreateNewObjectWithUsedKey);
          return createNewObject(inputRepoObject, timestamp, creationDate);

        case VERSION:
          if (existingRepoObject == null)
            throw new RepoException(RepoException.Type.CantCreateVersionWithNoOrig);
          return updateObject(inputRepoObject, timestamp, existingRepoObject, creationDate);

        case AUTO:
          if (existingRepoObject == null)
            return createNewObject(inputRepoObject, timestamp, creationDate);
          else
            return updateObject(inputRepoObject, timestamp, existingRepoObject, creationDate);

        default:
          throw new RepoException(RepoException.Type.InvalidCreationMethod);
      }
    } finally {
      writeLock.unlock();
    }

  }

  private RepoObject createNewObject(InputRepoObject inputRepoObject,
                                 Timestamp timestamp,
                                 Timestamp cretationDateTime) throws RepoException {

    ObjectStore.UploadInfo uploadInfo = null;
    Integer versionNumber;
    Bucket bucket;

    RepoObject repoObject;

    boolean rollback = false;

    try {

      try {
        sqlService.getConnection();
        bucket = sqlService.getBucket(inputRepoObject.getBucketName());
      } catch (SQLException e) {
        throw new RepoException(e);
      }

      if (bucket == null)
        throw new RepoException(RepoException.Type.BucketNotFound);

      InputStream content = inputRepoObject.getUploadedInputStream();
      uploadInfo = objectStore.uploadTempObject(content);

      try {
        content.close();
      } catch (IOException e) {
        throw new RepoException(e);
      }

      if (uploadInfo.getSize() == 0) {
        throw new RepoException(RepoException.Type.ObjectDataEmpty);
      }

      try {
        versionNumber = sqlService.getObjectNextAvailableVersion(inputRepoObject.getBucketName(),
                                                                  inputRepoObject.getKey());
      } catch (SQLException e) {
        throw new RepoException(e);
      }

      repoObject = new RepoObject(inputRepoObject.getKey(), bucket.getBucketId(),
          inputRepoObject.getBucketName(), Status.USED);
      repoObject.setDownloadName(inputRepoObject.getDownloadName());
      repoObject.setContentType(inputRepoObject.getContentType());
      repoObject.setUserMetadata(inputRepoObject.getUserMetadata());
      repoObject.setTag(inputRepoObject.getTag());
      repoObject.setChecksum(uploadInfo.getChecksum());
      repoObject.setTimestamp(timestamp);
      repoObject.setSize(uploadInfo.getSize());
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
        if (Boolean.FALSE.equals(objectStore.saveUploadedObject(new Bucket(inputRepoObject.getBucketName()),
            uploadInfo, repoObject))) {
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
        sqlRollback("object " + inputRepoObject.getBucketName() + ", " + inputRepoObject.getKey());
        // TODO: handle objectStore rollback, or not?
      }

      sqlReleaseConnection();
    }

    return repoObject;
  }

  private RepoObject updateObject(
      InputRepoObject inputRepoObject,
                                  Timestamp timestamp,
                                  RepoObject repoObject,
                                  Timestamp cretationDateTime) throws RepoException {

    ObjectStore.UploadInfo uploadInfo = null;
    boolean rollback = false;

    RepoObject newRepoObject = null;

    try {

      newRepoObject = createNewRepoObjectForUpate(inputRepoObject, repoObject, timestamp, cretationDateTime);

      sqlService.getConnection();

      if (sqlService.getBucket(inputRepoObject.getBucketName()) == null)
        throw new RepoException(RepoException.Type.BucketNotFound);

      InputStream uploadedInputStream = inputRepoObject.getUploadedInputStream();
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
          if (Boolean.FALSE.equals(objectStore.saveUploadedObject(new Bucket(inputRepoObject.getBucketName()), uploadInfo, newRepoObject))) {
            throw new RepoException("Error saving content to object store");
          }
        }
      }

      newRepoObject.setVersionChecksum(checksumGenerator.generateVersionChecksum(newRepoObject));

      // if the new object and the last version of the object are similar, we just return the last version of the object
      if (repoObject.areSimilar(newRepoObject)){
        rollback = false; // if we are not modifying the DB and reached this point, then the operations were successfully performed
        return repoObject;
      }

      newRepoObject.setVersionNumber(sqlService.getObjectNextAvailableVersion(inputRepoObject.getBucketName(), newRepoObject.getKey()));
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
        sqlRollback("object " + inputRepoObject.getBucketName() + ", " + repoObject.getKey());
        // TODO: handle objectStore rollback, or not?
      }

      sqlReleaseConnection();

    }

    return newRepoObject;
  }

  /**
   * Create a {@link org.plos.repo.models.RepoObject} to be updated. If any of the properties from <code>inputRepoObject</code> is null,
   * it gets that properties from <code>repoObject</code>
   * @param inputRepoObject a single {@link org.plos.repo.models.input.InputRepoObject} used as input for the new repo object
   * @param repoObject a single {@link org.plos.repo.models.RepoObject} used to obtain the values for the new repo object that
   *                   are null in <code>inputRepoObjecy</code>
   * @param timestamp a valid timestamp for the new repo object
   * @param cretationDateTime a valid creation date time for the new repo objec
   * @return a new repo object
   */
  private RepoObject createNewRepoObjectForUpate(InputRepoObject inputRepoObject, RepoObject repoObject, Timestamp timestamp, Timestamp cretationDateTime){
    // TODO: if the object is equals to the last version, do we need to update the timestamp?

    RepoObject newRepoObject =
        new RepoObject(repoObject.getKey(), repoObject.getBucketId(), inputRepoObject.getBucketName(), Status.USED);
    newRepoObject.setTimestamp(timestamp);
    newRepoObject.setCreationDate(cretationDateTime);

    // copy over values from previous object, if they are not specified in the request
    if (inputRepoObject.getContentType() == null){
      newRepoObject.setContentType(repoObject.getContentType());
    } else {
      newRepoObject.setContentType(inputRepoObject.getContentType());
    }

    if (inputRepoObject.getDownloadName() == null){
      newRepoObject.setDownloadName(repoObject.getDownloadName());
    } else {
      newRepoObject.setDownloadName(inputRepoObject.getDownloadName());
    }

    if (inputRepoObject.getUserMetadata() == null) {
      newRepoObject.setUserMetadata(repoObject.getUserMetadata());
    } else {
      newRepoObject.setUserMetadata(inputRepoObject.getUserMetadata());
    }

    if (inputRepoObject.getTag() == null) {
      newRepoObject.setTag(repoObject.getTag());
    } else {
      newRepoObject.setTag(inputRepoObject.getTag());
    }

    return newRepoObject;

  }

   @Override
  public Logger getLog() {
    return log;
  }

}
