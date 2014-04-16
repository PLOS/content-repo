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

import com.google.common.base.Joiner;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.glassfish.jersey.media.multipart.FormDataParam;
import org.plos.repo.models.Bucket;
import org.plos.repo.models.Object;
import org.plos.repo.service.ObjectStore;
import org.plos.repo.service.SqlService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.GenericEntity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.InputStream;
import java.sql.Timestamp;
import java.util.Date;
import java.util.List;

//@Component
@Path("/objects")
public class ObjectCrudController {

  private static final Logger log = LoggerFactory.getLogger(ObjectCrudController.class);

  private static final Joiner REPROXY_URL_JOINER = Joiner.on(' ');

  private static final int REPROXY_CACHE_FOR_VALUE = 6 * 60 * 60; // TODO: Make configurable

  private static final String REPROXY_CACHE_FOR_HEADER =
      REPROXY_CACHE_FOR_VALUE + "; Last-Modified Content-Type Content-Disposition";

  @Inject
  private ObjectStore objectStore;

  @Inject
  private SqlService sqlService;


  // TODO: check at startup that db is in sync with objectStore ?

  private static Gson gson = new Gson();

  private String objectToJsonString(Object object, boolean includeVersions) {
    JsonObject jsonObject = gson.toJsonTree(object).getAsJsonObject();

    if (includeVersions)
      jsonObject.add("versions", gson.toJsonTree(sqlService.listObjectVersions(object)).getAsJsonArray());

    return jsonObject.toString();
  }

  @GET
  public Response listAllObjects() throws Exception {
    return Response.status(Response.Status.OK).entity(
        new GenericEntity<List<Object>>(sqlService.listAllObject()){}).build();
  }

//  @RequestMapping(value="{bucketName}", method=RequestMethod.GET)
//  public @ResponseBody List<Object> listObjectsInBucket(@PathVariable String bucketName) throws Exception {
//    return sqlService.listObjectsInBucket(bucketName);
//  }

//  private boolean clientSupportsReproxy(HttpServletRequest request) {
//    Enumeration<?> headers = request.getHeaders("X-Proxy-Capabilities");
//    if (headers == null) {
//      return false;
//    }
//    while (headers.hasMoreElements()) {
//      if ("reproxy-file".equals(headers.nextElement())) {
//        return true;
//      }
//    }
//    return false;
//  }

  private boolean clientSupportsReproxy(String requestXProxy) {

    if (requestXProxy == null)
      return false;

    if (requestXProxy.equals("reproxy-file")) // TODO: move this and others to constants
      return true;

    return false;
  }

  @GET @Path("{bucketName}")
  public Response read(@PathParam("bucketName") String bucketName,
                       @QueryParam("key") String key,
                       @QueryParam("version") Integer version,
                       @QueryParam("fetchMetadata") Boolean fetchMetadata,
                       @HeaderParam("X-Proxy-Capabilities") String requestXProxy
  ) throws Exception {

    org.plos.repo.models.Object object;
    if (version == null)
      object = sqlService.getObject(bucketName, key);
    else
      object = sqlService.getObject(bucketName, key, version);

    if (object == null)
      return Response.status(Response.Status.NOT_FOUND).build();

    // if they want the metadata

    if (fetchMetadata != null && fetchMetadata) {
      return Response.status(Response.Status.OK).type(MediaType.APPLICATION_JSON_TYPE).entity(objectToJsonString(object, version == null)).build();
    }

    // if they want redirect URLs

    if (clientSupportsReproxy(requestXProxy) && objectStore.hasXReproxy()) {

      // if the urls are in the database use that first
      String urls = object.urls;

      // if not, check the filestore
      if (object.urls == null || object.urls.isEmpty())
        urls = REPROXY_URL_JOINER.join(objectStore.getRedirectURLs(object));

      return Response.status(Response.Status.OK).header("X-Reproxy-URL", urls).header("X-Reproxy-Cache-For", REPROXY_CACHE_FOR_HEADER).build();

    }

    // else assume they want the binary data

    String exportFileName = "content";

    if (object.downloadName != null)
      exportFileName = object.downloadName;
    else if (ObjectStore.isValidFileName(object.key))
      exportFileName = object.key;


    String contentType = object.contentType;

    if (object.contentType == null || object.contentType.isEmpty())
      contentType = MediaType.APPLICATION_OCTET_STREAM;

    InputStream is = objectStore.getInputStream(object);

    // TODO: find out if Jersey is closing the InputStream

    Response response = Response.ok(is, contentType).header("Content-Disposition", "inline; filename=" + exportFileName).build();

//      OutputStream os = response.getOutputStream();
//      IOUtils.copy(is, os);
//      response.flushBuffer();
//      is.close();
//      os.close();

    return response;

  }

  @DELETE
  @Path("{bucketName}")
  public Response delete(@PathParam("bucketName") String bucketName,
                         @QueryParam("key") String key,
                         @QueryParam("version") int version) throws Exception {

    if (sqlService.markObjectDeleted(key, bucketName, version) == 0)
      return Response.status(Response.Status.NOT_FOUND).entity("Error: Can not find object in database.").build();

    // NOTE: we no longer delete objects from the object store

    // delete it from the object store if it is no longer referenced in the database
//    if (!sqlService.objectInUse(bucketName, checksum) && !objectStore.deleteObject(objectStore.getObjectLocationString(bucketName, checksum)))
//      return new ResponseEntity<>("Error: There was a problem deleting the object from the filesystem.", HttpStatus.NOT_MODIFIED);

    return Response.status(Response.Status.OK).entity(key + " version " + version + " deleted").build();
  }

  @POST
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  public Response createOrUpdate(@FormDataParam("key") String key,
                                 @FormDataParam("bucketName") String bucketName,
                                 @FormDataParam("contentType") String contentType,
                                 @FormDataParam("downloadName") String downloadName,
                                 @FormDataParam("create") String create,
                                 @FormDataParam("file") InputStream uploadedInputStream
  ) throws Exception {

    Object existingObject = sqlService.getObject(bucketName, key);

    if (create.equalsIgnoreCase("new")) {
      return create(key, bucketName, contentType, downloadName, uploadedInputStream, existingObject);
    } else if (create.equalsIgnoreCase("version")) {
      return update(bucketName, contentType, downloadName, uploadedInputStream, existingObject);
    } else if (create.equalsIgnoreCase("auto")) {
      if (existingObject == null)
        return create(key, bucketName, contentType, downloadName, uploadedInputStream, null);
      else
        return update(bucketName, contentType, downloadName, uploadedInputStream, existingObject);
    }

    return Response.status(Response.Status.BAD_REQUEST).entity("Invalid create flag").build();
  }

  private Response create(String key,
                          String bucketName,
                          String contentType,
                          String downloadName,
                          InputStream uploadedInputStream,
                          Object existingObject) throws Exception {

    Integer bucketId = sqlService.getBucketId(bucketName);

    if (uploadedInputStream == null)
      return Response.status(Response.Status.PRECONDITION_FAILED).entity("Error: A file must be specified for uploading.").build();

    if (bucketId == null)
      return Response.status(Response.Status.NOT_FOUND).entity("Error: Can not find bucket " + bucketName).build();

    if (existingObject != null)
      return Response.status(Response.Status.CONFLICT).entity("Error: Attempting to create an object with a key that already exists.").build();

    ObjectStore.UploadInfo uploadInfo;
    try {
      uploadInfo = objectStore.uploadTempObject(uploadedInputStream);
    } catch (Exception e) {
      log.error("Error during upload", e);
      return Response.status(Response.Status.PRECONDITION_FAILED).entity("Error: A problem occurred while uploading the file.").build();
    }

    Integer versionNumber = sqlService.getNextAvailableVersionNumber(bucketName, key);

    Object object = new Object(null, key, uploadInfo.getChecksum(), new Timestamp(new Date().getTime()), downloadName, contentType, uploadInfo.getSize(), null, null, bucketId, bucketName, versionNumber, Object.Status.USED);

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
      objectStore.saveUploadedObject(new Bucket(bucketName), uploadInfo, object);
      object.urls = REPROXY_URL_JOINER.join(objectStore.getRedirectURLs(object));
    }

    // add a record to the DB

    sqlService.insertObject(object); // TODO: deal with 0 return values

    // TODO: explicitly cast the JSON ?
    return Response.status(Response.Status.CREATED).entity(objectToJsonString(object, false)).build();
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
                          InputStream uploadedInputStream,
                          Object object) throws Exception {

    Integer bucketId = sqlService.getBucketId(bucketName);

    if (bucketId == null)
      return Response.status(Response.Status.NOT_FOUND).entity("Error: Can not find bucket " + bucketName).build();

    if (object == null)
      return Response.status(Response.Status.NOT_ACCEPTABLE).entity("Error: Attempting to create a new version of an non-existing object.").build();

    // copy over values from previous object, if they are not specified in the request
    if (contentType != null)
      object.contentType = contentType;

    if (downloadName != null)
      object.downloadName = downloadName;

    // TODO: wrap this in a transaction since versionNumber is being updated ?

    object.timestamp = new Timestamp(new Date().getTime());
    object.versionNumber++;
    object.id = null;  // remove this since it refers to the old object

    if (uploadedInputStream == null) {
      object.urls = REPROXY_URL_JOINER.join(objectStore.getRedirectURLs(object));
      sqlService.insertObject(object); // TODO: deal with 0 return values

      return Response.status(Response.Status.OK).entity(objectToJsonString(object, false)).build();
    }

    ObjectStore.UploadInfo uploadInfo;
    try {
      uploadInfo = objectStore.uploadTempObject(uploadedInputStream);
    } catch (Exception e) {
      log.error("Error during upload", e);

      return Response.status(Response.Status.PRECONDITION_FAILED).entity("Error: A problem occurred while uploading the file.").build();
    }

    object.urls = "";

    // determine if the object should be added to the store or not
    object.checksum = uploadInfo.getChecksum();
    object.size = uploadInfo.getSize();
    if (objectStore.objectExists(object)) {
      objectStore.deleteTempUpload(uploadInfo);
    } else {
      objectStore.saveUploadedObject(new Bucket(bucketName), uploadInfo, object);
    }

    object.urls = REPROXY_URL_JOINER.join(objectStore.getRedirectURLs(object));

    // add a record to the DB
    sqlService.insertObject(object); // TODO: deal with 0 return values

    return Response.status(Response.Status.OK).entity(objectToJsonString(object, false)).build();
  }

}
