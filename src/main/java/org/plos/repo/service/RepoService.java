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

  @Inject
  private JournalService journalService;

  public List<Bucket> listBuckets() throws RepoException {
    try {
      sqlService.getReadOnlyConnection();
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
    boolean bucketCreation = false;

    Bucket bucket = new Bucket(name);
    Bucket newBucket = null;
    try {

      if (!ObjectStore.isValidFileName(name))
        throw new RepoException(RepoException.Type.IllegalBucketName);

      sqlService.getConnection();
      rollback = true;

      if (sqlService.getBucket(name) != null)
        throw new RepoException(RepoException.Type.BucketAlreadyExists);


      if (Boolean.TRUE.equals(objectStore.bucketExists(bucket)))
        throw new RepoException("Bucket exists in object store but not in database: " + name);

      bucketCreation = objectStore.createBucket(bucket);

      if (Boolean.FALSE.equals(bucketCreation))
        throw new RepoException("Unable to create bucket in object store: " + name);

      Timestamp creationDate = creationDateTimeString != null ?
          Timestamp.valueOf(creationDateTimeString) : new Timestamp(new Date().getTime());

      if (!sqlService.insertBucket(bucket, creationDate)) {
        throw new RepoException("Unable to create bucket in database: " + name);
      }

      newBucket = sqlService.getBucket(name);

      sqlService.transactionCommit();
      rollback = false;

    } catch (SQLException e) {
      throw new RepoException(e);
    } finally {

      if (rollback) {
        sqlRollback("bucket " + name);

        if (bucketCreation) {
          objectStore.deleteBucket(bucket);
          // TODO: check to make sure objectStore.deleteBucket didnt fail
        }

      }

      sqlReleaseConnection();
      writeLock.unlock();

    }
    if(!rollback){
        journalService.createBucket(name);
      
    }
    return newBucket;
  }

  /**
   * Delete the given bucket <code>name</code> if it does not contain any active or deleted objects.
   * When deleting the bucket, it also deletes all purge objects and collections (no matter the status)
   * contained in it.
   * @param name a string representing the bucket to be deleted.
   * @throws RepoException if the given bucket is not found in the DB
   *                       if bucket is found in the DB, but it does not exists in the object store
   *                       if the bucket can't be deleted because active or deleted objects exist in it
   *                       if there's an error when attempting to delete the bucket from the DB or the object store
   */
  public void deleteBucket(String name) throws RepoException {

    Lock writeLock = this.rwLocks.get(name).writeLock();
    writeLock.lock();

    boolean rollback = false;
    boolean bucketDeletion = false;

    Bucket bucket = new Bucket(name);

    try {
      sqlService.getConnection();
      rollback = true;

      if (sqlService.getBucket(name) == null)
        throw new RepoException(RepoException.Type.BucketNotFound);

      if (Boolean.FALSE.equals(objectStore.bucketExists(bucket)))
        throw new RepoException("Bucket exists in database but not in object store: " + name);

      if (sqlService.listObjects(name, 0, 1, true, false, null).size() != 0)
        throw new RepoException(RepoException.Type.CantDeleteNonEmptyBucket);

      // remove all table references for objects & collections in the bucket
      if (sqlService.removeBucketContent(name) == 0)
        throw new RepoException("Unable to delete bucket in database: " + name);

      bucketDeletion = objectStore.deleteBucket(bucket);
      if (!bucketDeletion){
        throw new RepoException("Unable to delete bucket in object store: " + name);
      }

      sqlService.transactionCommit();
      rollback = false;

    } catch (SQLException e) {
      throw new RepoException(e);
    } finally {

      if (rollback) {
        sqlRollback("bucket " + name);

        if (bucketDeletion) {
          objectStore.createBucket(bucket);
          // TODO: validate objectStore.createBucket return values
        }

      }

      sqlReleaseConnection();
      writeLock.unlock();
    }

    if(!rollback) {
      journalService.deleteBucket(name);

    }
  }

  public boolean serverSupportsReproxy() {
    return objectStore.hasXReproxy();
  }

  /**
   * List objects filtered by the parameters passed to the method.
   * @param bucketName a single string representing the bucket in which the objects must be contained
   * @param offset a single number used to paginate the response
   * @param limit a single number used to paginate the response, indicating the limit of rows returned
   * @param includeDeleted a boolean indicating whether to include the objects mark as deleted
   * @param includePurged a boolean indicating whether to include the objects mark as purged
   * @param tag a single integer used to filter the objects matching the given tag. If the label is null,
   *            no filter is applied on such property
   * @return a list of objects {@link org.plos.repo.models.RepoObject}
   * @throws RepoException if bucket name has not been entered
   *                       if the given bucket is not found in the DB
   *                       if there's an error when attempting to query the DB
   */
  public List<RepoObject> listObjects(String bucketName, Integer offset, Integer limit, boolean includeDeleted,
                                      boolean includePurged, String tag) throws RepoException {

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

      sqlService.getReadOnlyConnection();

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
      sqlService.getReadOnlyConnection();

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
      sqlService.getReadOnlyConnection();
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
      return objectStore.getRedirectURLs(repoObject);
    } catch (Exception e) {
      throw new RepoException(e);
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
    InputStream content = null;
    try {
      content = objectStore.getInputStream(repoObject);
    } catch (Exception e) {
      log.error("Error retrieving content for object.  Key: {} , bucketName: {} , versionChecksum: {} . Error: {}",
          repoObject.getKey(),
          repoObject.getBucketName(),
          repoObject.getVersionChecksum(),
          e.getMessage());
      throw new RepoException(e);
    }
    if (content == null){
      log.error("Error retrieving content for object. Content not found.  Key: {} , bucketName: {} , versionChecksum: {} ",
          repoObject.getKey(),
          repoObject.getBucketName(),
          repoObject.getVersionChecksum());
      throw new RepoException(RepoException.Type.ObjectContentNotFound);
    }
    return content;
  }

  /**
   * Mark object as deleted or purged, depending on the <code>purge</code> value.
   * @param bucketName a single string representing the bucket in which the objects must be contained
   * @param key a single string representing the object to be deleted/purged
   * @param purge a boolean indicating whether to purge or delete the object. When equals to true,
   *              the object must be purged
   * @param elementFilter a {@link org.plos.repo.models.input.ElementFilter} is used to identify which object
   *                      for the given <code>key</code> must be changed
   * @throws RepoException see {@link org.plos.repo.service.RepoService#deleteObject(String, String, org.plos.repo.models.input.ElementFilter, org.plos.repo.models.Status)}
   */
  public void deleteObject(String bucketName, String key, Boolean purge, ElementFilter elementFilter) throws RepoException {
    if (purge){
      deleteObject(bucketName, key, elementFilter, Status.PURGED);
    } else {
      deleteObject(bucketName, key, elementFilter, Status.DELETED);
    }

  }

  /**
   * Delete or Purge an object identify with <code>bucketName</code>, <code>key</code> and <code>elementFilter</code>
   * depending on the <code>status</code>. Before deleting/purging and object, it validates if the object identify
   * exists (has not been purged)
   * @param bucketName  a single string representing the bucket in which the objects must be contained
   * @param key a single string representing the object to be deleted/purged
   * @param elementFilter a {@link org.plos.repo.models.input.ElementFilter} is used to identify which object
   *                      for the given <code>key</code> must be changed
   * @param status a {@link org.plos.repo.models.input.ElementFilter} is used to identify which object
   *                      for the given <code>key</code> must be changed
   * @throws RepoException if bucket name has not been entered
   *                       if the elementFilter is null or empty
   *                       if the object does not exists
   */
  public void deleteObject(String bucketName, String key, ElementFilter elementFilter, Status status ) throws RepoException {

    Lock writeLock = this.rwLocks.get(bucketName + key).writeLock();
    writeLock.lock();

    boolean rollback = false;

    try {

      if (key == null)
        throw new RepoException(RepoException.Type.NoKeyEntered);

      if ((elementFilter == null || elementFilter.isEmpty())) {
        throw new RepoException(RepoException.Type.NoFilterEntered);
      }

      sqlService.getConnection();
      rollback = true;

      if (elementFilter.getTag() != null & elementFilter.getVersionChecksum() == null & elementFilter.getVersion() == null){
        if (sqlService.listObjects(bucketName, 0, 10, false, false, elementFilter.getTag()).size() > 1){
          throw new RepoException(RepoException.Type.MoreThanOneTaggedObject);
        }
      }

      RepoObject repoObject = sqlService.getObject(bucketName, key, elementFilter.getVersion(), elementFilter.getVersionChecksum(), elementFilter.getTag(), true, false);
      if (repoObject == null) {
        throw new RepoException(RepoException.Type.ObjectNotFound);
      }

      if (Status.DELETED.equals(status)){

        int objectDeteled = sqlService.markObjectDeleted(key, bucketName, elementFilter.getVersion(),
            elementFilter.getVersionChecksum(), elementFilter.getTag());

        if (objectDeteled == 0)
          throw new RepoException(RepoException.Type.ObjectNotFound);

      } else if (Status.PURGED.equals(status)){
        purgeObjectContentAndDb(repoObject, elementFilter);
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
    if(!rollback){
      journalService.deletePurgeObject(bucketName, key, status, elementFilter);
      
    }
  }

  /**
   * Updates the status as PURGED, and try to delete the content of the object from mogile. To do so,
   *  it checks whether other USED or DELETED objects are pointing to the same content. If there’s no other
   *  objects pointing to it, or all the objects pointing to that content are marked as PURGE, it proceeds
   *  to remove the content.

   * @param repoObject object to be purged
   * @param elementFilter a {@link org.plos.repo.models.input.ElementFilter} is used to identify which object
   *                      for the given <code>key</code> must be changed
   * @throws RepoException if the content of the object is not found
   *                       if the metadata of the object is not found in the DB
   */
  private void purgeObjectContentAndDb(RepoObject repoObject, ElementFilter elementFilter)  throws RepoException {

    try {

      try (InputStream content = getObjectInputStream(repoObject)){
        if (content == null){
          throw new RepoException(RepoException.Type.ObjectNotFound);
        }
      } catch (IOException e) {
        throw new RepoException(e);
      }

      int objectPurged = sqlService.markObjectPurged(repoObject.getKey(), repoObject.getBucketName(),
                            elementFilter.getVersion(), elementFilter.getVersionChecksum(), elementFilter.getTag());

      if (objectPurged == 0){
        throw new RepoException(RepoException.Type.ObjectNotFound);
      }

      // verify if any other USED or DELETED objects has a reference to the same file. If it is the last reference to the object, remove it from the system,
      // if not, just mark the record in the DB as purge
      if (sqlService.countUsedAndDeletedObjectsReference(repoObject.getBucketName(), repoObject.getChecksum()) == 0 ){
        Boolean removed = objectStore.deleteObject(repoObject);
        if (!removed){
          throw new RepoException(RepoException.Type.ObjectNotFound);
        }
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

    RepoObject repoObject = null;

    boolean rollback = false;

    try {

      try {
        sqlService.getConnection();
        rollback = true;
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

    if(!rollback && repoObject != null) {
      journalService.createObject(bucketName, key, repoObject.getVersionChecksum());
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
      rollback = true;

      if (sqlService.getBucket(bucketName) == null)
        throw new RepoException(RepoException.Type.BucketNotFound);
      // copy over values from previous object, if they are not specified in the request
      if (contentType == null)
        newRepoObject.setContentType(repoObject.getContentType());
      if (downloadName == null)
        newRepoObject.setDownloadName(repoObject.getDownloadName());

      if (uploadedInputStream == null) {
      // handle metadata-only update, the content would be the same as the last version of the object
        newRepoObject.setChecksum(repoObject.getChecksum());
        newRepoObject.setSize(repoObject.getSize());
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
    if(!rollback) {
      journalService.updateObject(bucketName, repoObject.getKey(), newRepoObject.getVersionChecksum());

    }
    return newRepoObject;
  }

   @Override
  public Logger getLog() {
    return log;
  }

}
