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

import javax.inject.Inject;
import javax.ws.rs.core.MediaType;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLEncoder;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;

/**
 * This service handles all communication to the objectstore and sqlservice
 */
public class RepoService {

  @Inject
  private ObjectStore objectStore;

  @Inject
  private SqlService sqlService;

  public List<Bucket> listBuckets() throws SQLException {
    return sqlService.listBuckets();
  }

  public void createBucket(String name) throws RepoException, SQLException {

    if (sqlService.getBucketId(name) != null)
      throw new RepoException("Bucket already exists");

    if (!ObjectStore.isValidFileName(name))
      throw new RepoException("Unable to create bucket. Name contains illegal characters: " + name);

    Bucket bucket = new Bucket(null, name);

    if (!objectStore.createBucket(bucket))
      throw new RepoException("Unable to create bucket " + name + " in object store");


    if (!sqlService.insertBucket(bucket)) {
      objectStore.deleteBucket(bucket);
      throw new RepoException("Unable to create bucket " + name + " in database");
    }

  }

  public void deleteBucket(String name) throws RepoException, SQLException, ClassNotFoundException {

    if (sqlService.getBucketId(name) == null)
      throw new ClassNotFoundException();

    if (sqlService.listObjectsInBucket(name).size() != 0)
      throw new RepoException("Cannot delete bucket " + name + " because it contains objects.");

    Bucket bucket = new Bucket(null, name);

    if (!objectStore.deleteBucket(bucket))
      throw new RepoException("There was a problem removing the bucket");

    if (sqlService.deleteBucket(name) == 0)
      throw new RepoException("No buckets deleted.");

  }

  public boolean serverSupportsReproxy() {
    return objectStore.hasXReproxy();
  }

  public List<Object> listObjects(String bucketName) throws RepoException, SQLException, ClassNotFoundException {

    if (bucketName == null)
      return sqlService.listAllObject();

    if (sqlService.getBucketId(bucketName) == null)
      throw new ClassNotFoundException();

    return sqlService.listObjectsInBucket(bucketName);
  }

  public Object getObject(String bucketName, String key, Integer version) throws SQLException, ClassNotFoundException {

    Object object;
    if (version == null)
      object = sqlService.getObject(bucketName, key);
    else
      object = sqlService.getObject(bucketName, key, version);

    if (object == null)
      throw new ClassNotFoundException();

    return object;
  }

  public List<Object> getObjectVersions(Object object) throws SQLException {
    return sqlService.listObjectVersions(object);
  }

  public URL[] getObjectReproxy(Object object) throws Exception {
    return objectStore.getRedirectURLs(object);
  }

  public String getObjectContentType(Object object) {
    String contentType = object.contentType;

    if (object.contentType == null || object.contentType.isEmpty())
      contentType = MediaType.APPLICATION_OCTET_STREAM;

    return contentType;
  }

  public String getObjectExportFileName(Object object) throws UnsupportedEncodingException {

    String exportFileName = object.key;

    if (object.downloadName != null)
      exportFileName = object.downloadName;

    return URLEncoder.encode(exportFileName, "UTF-8");

  }

  public InputStream getObjectInputStream(Object object) throws Exception {
    return objectStore.getInputStream(object);
  }

  public void deleteObject(String bucketName, String key, Integer version) throws ClassNotFoundException, SQLException {

    if (sqlService.markObjectDeleted(key, bucketName, version) == 0)
      throw new ClassNotFoundException();
  }

  public Object createNewObject(String key,
                                String bucketName,
                                String contentType,
                                String downloadName,
                                Timestamp timestamp,
                                InputStream uploadedInputStream) throws Exception {

    Integer bucketId = sqlService.getBucketId(bucketName);

    if (bucketId == null)
      throw new RepoException("Can not find bucket " + bucketName);

    ObjectStore.UploadInfo uploadInfo;

    uploadInfo = objectStore.uploadTempObject(uploadedInputStream);
    uploadedInputStream.close();

    if (uploadInfo.getSize() == 0) {
      objectStore.deleteTempUpload(uploadInfo);
      throw new RepoException("Uploaded data must be non-empty");
    }

    Integer versionNumber = sqlService.getNextAvailableVersionNumber(bucketName, key);

    Object object = new Object(null, key, uploadInfo.getChecksum(), timestamp, downloadName, contentType, uploadInfo.getSize(), null, bucketId, bucketName, versionNumber, Object.Status.USED);

    // determine if the object should be added to the store or not
    if (objectStore.objectExists(object)) {

//      if (FileUtils.contentEquals(tempFile, new File(objectStore.getObjectLocationString(bucketName, checksum)))) {
//        log.info("not adding object to store since content exists");
//      } else {
//        log.info("checksum collision!!");
//        status = HttpStatus.CONFLICT;
//      }

      // dont bother storing the file since the data already exists in the system
      objectStore.deleteTempUpload(uploadInfo);
    } else {
      if (!objectStore.saveUploadedObject(new Bucket(null, bucketName), uploadInfo, object)) {
        objectStore.deleteTempUpload(uploadInfo);
        throw new Exception("Error saving content to data store");
      }
    }

    // add a record to the DB

    if (sqlService.insertObject(object) == 0) {
      //objectStore.deleteObject(object);
      throw new Exception("Error saving content to database");
    }

    return object;
  }

  public Object updateObject(String bucketName,
                          String contentType,
                          String downloadName,
                          Timestamp timestamp,
                          InputStream uploadedInputStream,
                          Object object) throws Exception {

    Integer bucketId = sqlService.getBucketId(bucketName);

    if (bucketId == null)
      throw new RepoException("Can not find bucket " + bucketName);

    // copy over values from previous object, if they are not specified in the request
    if (contentType != null)
      object.contentType = contentType;

    if (downloadName != null)
      object.downloadName = downloadName;

    object.timestamp = timestamp;
    object.versionNumber = sqlService.getNextAvailableVersionNumber(bucketName, object.key);
    object.id = null;  // remove this since it refers to the old object

    ObjectStore.UploadInfo uploadInfo = objectStore.uploadTempObject(uploadedInputStream);
    uploadedInputStream.close();

    // handle metadata-only update
    if (uploadInfo.getSize() == 0) {
      objectStore.deleteTempUpload(uploadInfo);
      sqlService.insertObject(object); // TODO: deal with 0 return values

      return object;
    }

    // determine if the object should be added to the store or not
    object.checksum = uploadInfo.getChecksum();
    object.size = uploadInfo.getSize();
    if (objectStore.objectExists(object)) {
      objectStore.deleteTempUpload(uploadInfo);
    } else {
      if (!objectStore.saveUploadedObject(new Bucket(null, bucketName), uploadInfo, object)) {
        objectStore.deleteTempUpload(uploadInfo);
        throw new Exception("Error saving content to data store");
      }
    }

    // add a record to the DB

    if (sqlService.insertObject(object) == 0) {
      //objectStore.deleteObject(object);
      throw new Exception("Error saving content to database");
    }

    return object;
  }

}
