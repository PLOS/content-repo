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

package org.plos.repo.rest;

import java.io.InputStream;
import java.net.URLEncoder;
import java.sql.Timestamp;
import java.util.Date;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.GenericEntity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.junit.ComparisonFailure;

import org.apache.http.HttpStatus;
import org.glassfish.jersey.media.multipart.FormDataParam;
import org.plos.repo.models.Bucket;
import org.plos.repo.models.Object;
import org.plos.repo.service.ObjectStore;
import org.plos.repo.service.RepoInfoService;
import org.plos.repo.service.SqlService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.util.concurrent.Striped;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import com.wordnik.swagger.annotations.ApiResponse;
import com.wordnik.swagger.annotations.ApiResponses;

@Path("/objects")
@Api(value="/objects")
public class ObjectController {

  private static final Logger log = LoggerFactory.getLogger(ObjectController.class);

  private static final Joiner REPROXY_URL_JOINER = Joiner.on(' ');

  private static final int REPROXY_CACHE_FOR_VALUE = 6 * 60 * 60; // TODO: Make configurable

  private static final String REPROXY_CACHE_FOR_HEADER =
      REPROXY_CACHE_FOR_VALUE + "; Last-Modified Content-Type Content-Disposition";

  private static final String REPROXY_HEADER_URL = "X-Reproxy-URL";

  private static final String REPROXY_HEADER_CACHE_FOR = "X-Reproxy-Cache-For";

  private static final String REPROXY_HEADER_FILE = "reproxy-file";

  private Striped<ReadWriteLock> rwLocks = Striped.lazyWeakReadWriteLock(25);

  @Inject
  private ObjectStore objectStore;

  @Inject
  private SqlService sqlService;

  @Inject
  private RepoInfoService repoInfoService;


  // TODO: check at startup that db is in sync with objectStore ? bill says write a python script instead


  @GET
  @ApiOperation(value = "List objects", response = Object.class, responseContainer = "List")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
  public Response listObjects(
      @ApiParam(required = false) @QueryParam("bucketName") String bucketName) throws Exception {


    // TODO: allow filtering on object status, or add paging?
    //   and/or write list to a stream instead so it does take up memory

    if (bucketName == null)
      return Response.status(Response.Status.OK).entity(
        new GenericEntity<List<Object>>(sqlService.listAllObject()){}).build();

    if (sqlService.getBucketId(bucketName) == null)
      return Response.status(Response.Status.NOT_FOUND)
          .entity("Bucket not found").type(MediaType.TEXT_PLAIN).build();

    return Response.status(Response.Status.OK).entity(
        new GenericEntity<List<Object>>(sqlService.listObjectsInBucket(bucketName)) {
        }).build();
  }

  @GET @Path("/{bucketName}")
  @ApiOperation(value = "Fetch an object or its metadata", response = Object.class)
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
  public Response read(@ApiParam(required = true) @PathParam("bucketName") String bucketName,
                       @ApiParam(required = true) @QueryParam("key") String key,
                       @QueryParam("version") Integer version,
                       @QueryParam("fetchMetadata") Boolean fetchMetadata,
                       @ApiParam(value = "If set to 'reproxy-file' then it will attempt to return a header representing a redirected object URL")
                       @HeaderParam("X-Proxy-Capabilities") String requestXProxy
  ) throws Exception {

    Lock readLock = this.rwLocks.get(bucketName + key).readLock();
    readLock.lock();
    try {
      org.plos.repo.models.Object object;
      if (version == null)
        object = sqlService.getObject(bucketName, key);
      else
        object = sqlService.getObject(bucketName, key, version);

      if (object == null)
        return Response.status(Response.Status.NOT_FOUND).build();

      repoInfoService.incrementReadCount();

      // if they want the metadata

      if (fetchMetadata != null && fetchMetadata) {
        object.versions = sqlService.listObjectVersions(object);
        return Response.status(Response.Status.OK).entity(object).build();
      }

      // if they want redirect URLs

      if ( requestXProxy != null && requestXProxy.equals(REPROXY_HEADER_FILE) && objectStore.hasXReproxy()) {

        return Response.status(Response.Status.OK).header(REPROXY_HEADER_URL,
            REPROXY_URL_JOINER.join(objectStore.getRedirectURLs(object)))
            .header(REPROXY_HEADER_CACHE_FOR, REPROXY_CACHE_FOR_HEADER).build();

      }

      // else assume they want the binary data

      String exportFileName = object.key;

      if (object.downloadName != null)
        exportFileName = object.downloadName;

      exportFileName = URLEncoder.encode(exportFileName, "UTF-8");

      String contentType = object.contentType;

      if (object.contentType == null || object.contentType.isEmpty())
        contentType = MediaType.APPLICATION_OCTET_STREAM;

      InputStream is = objectStore.getInputStream(object);

      Response response = Response.ok(is, contentType)
          .header("Content-Disposition", "inline; filename=" + exportFileName).build();

      // post: container will close this input stream.

      return response;
    } finally {
      readLock.unlock();
    }
  }

  @DELETE
  @Path("/{bucketName}")
  @ApiOperation(value = "Delete an object")
  @ApiResponses(value = {
      @ApiResponse(code = HttpStatus.SC_BAD_REQUEST, message = "The object was unable to be deleted (see response text for more details)")
  })
  public Response delete(@ApiParam(required = true) @PathParam("bucketName") String bucketName,
                         @ApiParam(required = true) @QueryParam("key") String key,
                         @ApiParam(required = true) @QueryParam("version") Integer version) throws Exception {

    Lock writeLock = this.rwLocks.get(bucketName + key).writeLock();
    writeLock.lock();
    try {
      if (key == null)
        return Response.status(Response.Status.BAD_REQUEST)
            .entity("No key entered").type(MediaType.TEXT_PLAIN).build();

      if (version == null)
        return Response.status(Response.Status.BAD_REQUEST)
            .entity("No version entered").type(MediaType.TEXT_PLAIN).build();

      if (sqlService.markObjectDeleted(key, bucketName, version) == 0)
        return Response.status(Response.Status.NOT_FOUND)
            .entity("Can not find object in database.").type(MediaType.TEXT_PLAIN).build();

      // NOTE: we no longer delete objects from the object store

      // delete it from the object store if it is no longer referenced in the database
  //    if (!sqlService.objectInUse(bucketName, checksum) && !objectStore.deleteObject(objectStore.getObjectLocationString(bucketName, checksum)))
  //      return new ResponseEntity<>("Error: There was a problem deleting the object from the filesystem.", HttpStatus.NOT_MODIFIED);

      return Response.status(Response.Status.OK)
          .entity(key + " version " + version + " deleted").type(MediaType.TEXT_PLAIN).build();
    } finally {
      writeLock.unlock();
    }
  }

  @POST
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  @ApiOperation(value = "Create a new object or a new version of an existing object",
      notes = "Set the create field to 'new' object if the object you are inserting is not already in the repo. If you want to create a new version of an existing object set create to 'version'. Setting create to 'auto' automagically determines if the object should be new or versioned. However 'auto' should only be used by the ambra-file-store. In addition you may optionally specify a timestamp for object creation time. This feature is for migrating from an existing content store. Note that the timestamp must conform to this format: yyyy-[m]m-[d]d hh:mm:ss[.f...]")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
  @ApiResponses(value = {
      @ApiResponse(code = HttpStatus.SC_BAD_REQUEST, message = "The object was unable to be created (see response text for more details)"),
      @ApiResponse(code = HttpStatus.SC_PRECONDITION_FAILED, message = "The object was unable to be created (see response text for more details)"),
      @ApiResponse(code = HttpStatus.SC_NOT_ACCEPTABLE, message = "The object was unable to be created (see response text for more details)")
  })
  public Response createOrUpdate(
      @ApiParam(required = true) @FormDataParam("key") String key,
      @ApiParam(required = true) @FormDataParam("bucketName") String bucketName,
      @ApiParam(value = "MIME type") @FormDataParam("contentType") String contentType,
      @ApiParam(value = "name of file when downloaded", required = false)
        @FormDataParam("downloadName") String downloadName,
      @ApiParam(value = "creation method", allowableValues = "new,version,auto", defaultValue = "new",
required = true)
        @FormDataParam("create") String create,
      @ApiParam(value = "creation time", required = false)
        @FormDataParam("timestamp") String timestampString,
      @ApiParam(required = false)
        @FormDataParam("file") InputStream uploadedInputStream
  ) throws Exception {

    Lock writeLock = this.rwLocks.get(bucketName + key).writeLock();
    writeLock.lock();
    try {
      if (key == null)
        return Response.status(Response.Status.BAD_REQUEST)
            .entity("No key entered").type(MediaType.TEXT_PLAIN).build();

      if (create == null)
        return Response.status(Response.Status.BAD_REQUEST)
            .entity("No create flag entered").type(MediaType.TEXT_PLAIN).build();

      if (bucketName == null)
        return Response.status(Response.Status.BAD_REQUEST)
            .entity("No bucket specified").type(MediaType.TEXT_PLAIN).build();

      Timestamp timestamp = new Timestamp(new Date().getTime());

      if (timestampString != null) {
        try {
          timestamp = Timestamp.valueOf(timestampString);
        } catch (Exception e) {
          return Response.status(Response.Status.BAD_REQUEST)
              .entity("Could not parse timestamp").type(MediaType.TEXT_PLAIN).build();
        }
      }

      repoInfoService.incrementWriteCount();

      Object existingObject = sqlService.getObject(bucketName, key);

      if (create.equalsIgnoreCase("new")) {
        if (existingObject != null)
          return Response.status(Response.Status.NOT_ACCEPTABLE)
              .entity("Attempting to create an object with a key that already exists.").type(MediaType.TEXT_PLAIN).build();

        return create(key, bucketName, contentType, downloadName, timestamp, uploadedInputStream);
      } else if (create.equalsIgnoreCase("version")) {
        return update(bucketName, contentType, downloadName, timestamp, uploadedInputStream, existingObject);
      } else if (create.equalsIgnoreCase("auto")) {
        if (existingObject == null)
          return create(key, bucketName, contentType, downloadName, timestamp, uploadedInputStream);
        else
          return update(bucketName, contentType, downloadName, timestamp, uploadedInputStream, existingObject);
        }

      return Response.status(Response.Status.BAD_REQUEST)
          .entity("Invalid create flag").type(MediaType.TEXT_PLAIN).build();
    } finally {
      writeLock.unlock();
    }
  }

  private Response create(String key,
                          String bucketName,
                          String contentType,
                          String downloadName,
                          Timestamp timestamp,
                          InputStream uploadedInputStream) throws Exception {

    Integer bucketId = sqlService.getBucketId(bucketName);

    if (uploadedInputStream == null)
      return Response.status(Response.Status.PRECONDITION_FAILED)
          .entity("A file must be specified for uploading.").type(MediaType.TEXT_PLAIN).build();

    if (bucketId == null)
      return Response.status(Response.Status.PRECONDITION_FAILED)
          .entity("Can not find bucket " + bucketName).type(MediaType.TEXT_PLAIN).build();

    ObjectStore.UploadInfo uploadInfo;
    try {
      uploadInfo = objectStore.uploadTempObject(uploadedInputStream);
      uploadedInputStream.close();

      if (uploadInfo.getSize() == 0) {
        objectStore.deleteTempUpload(uploadInfo);
        return Response.status(Response.Status.PRECONDITION_FAILED)
            .entity("Uploaded data must be non-empty").type(MediaType.TEXT_PLAIN).build();
      }

    } catch (Exception e) {
      log.error("Error during upload", e);
      return Response.status(Response.Status.PRECONDITION_FAILED)
          .entity("A problem occurred while uploading the file.").type(MediaType.TEXT_PLAIN).build();
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
        return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
            .entity("Error saving content to data store").type(MediaType.TEXT_PLAIN).build();
      }
    }

    // add a record to the DB

    if (sqlService.insertObject(object) == 0) {
      //objectStore.deleteObject(object);
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity("Error saving content to database").type(MediaType.TEXT_PLAIN).build();
    }

    return Response.status(Response.Status.CREATED).entity(object).build();
  }

  /**
   * Take an existing key, make a copy of it and modify it as needed. This means a new version
   * of an existing object. This new part of it could be the file itself, or some of its metadata.
   *
   * @param bucketName
   * @param contentType
   * @param downloadName
   * @param uploadedInputStream
   * @param object
   * @return
   * @throws Exception
   */
  private Response update(String bucketName,
                          String contentType,
                          String downloadName,
                          Timestamp timestamp,
                          InputStream uploadedInputStream,
                          Object object) throws Exception {

    Integer bucketId = sqlService.getBucketId(bucketName);

    if (bucketId == null)
      return Response.status(Response.Status.PRECONDITION_FAILED)
          .entity("Can not find bucket " + bucketName).type(MediaType.TEXT_PLAIN).build();

    if (object == null)
      return Response.status(Response.Status.NOT_ACCEPTABLE)
          .entity("Attempting to create a new version of an non-existing object.").type(MediaType.TEXT_PLAIN).build();

    // copy over values from previous object, if they are not specified in the request
    if (contentType != null)
      object.contentType = contentType;

    if (downloadName != null)
      object.downloadName = downloadName;

    // TODO: wrap this in a DB transaction since versionNumber is being updated ?

    object.timestamp = timestamp;
    object.versionNumber = sqlService.getNextAvailableVersionNumber(bucketName, object.key);
    object.id = null;  // remove this since it refers to the old object

    ObjectStore.UploadInfo uploadInfo;
    try {
      uploadInfo = objectStore.uploadTempObject(uploadedInputStream);
      uploadedInputStream.close();
    } catch (Exception e) {
      log.error("Error during upload", e);

      return Response.status(Response.Status.PRECONDITION_FAILED)
          .entity("A problem occurred while uploading the file.").type(MediaType.TEXT_PLAIN).build();
    }

    if (uploadedInputStream == null || uploadInfo.getSize() == 0) {
      objectStore.deleteTempUpload(uploadInfo);
      sqlService.insertObject(object); // TODO: deal with 0 return values

      return Response.status(Response.Status.CREATED).entity(object).build();
    }

    // determine if the object should be added to the store or not
    object.checksum = uploadInfo.getChecksum();
    object.size = uploadInfo.getSize();
    if (objectStore.objectExists(object)) {
      objectStore.deleteTempUpload(uploadInfo);
    } else {
      if (!objectStore.saveUploadedObject(new Bucket(null, bucketName), uploadInfo, object)) {
        objectStore.deleteTempUpload(uploadInfo);
        return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
            .entity("Error saving content to data store").type(MediaType.TEXT_PLAIN).build();
      }
    }

    // add a record to the DB

    if (sqlService.insertObject(object) == 0) {
      //objectStore.deleteObject(object);
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity("Error saving content to database").type(MediaType.TEXT_PLAIN).build();
    }

    return Response.status(Response.Status.CREATED).entity(object).build();
  }

}
