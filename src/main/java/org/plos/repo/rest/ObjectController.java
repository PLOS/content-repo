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
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import com.wordnik.swagger.annotations.ApiResponse;
import com.wordnik.swagger.annotations.ApiResponses;
import org.apache.http.HttpStatus;
import org.glassfish.jersey.media.multipart.FormDataParam;
import org.plos.repo.models.Object;
import org.plos.repo.service.RepoException;
import org.plos.repo.service.RepoInfoService;
import org.plos.repo.service.RepoService;
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
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.GenericEntity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.InputStream;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.List;

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

  @Inject
  private RepoService repoService;

  @Inject
  private RepoInfoService repoInfoService;


  // TODO: check at startup that db is in sync with objectStore ? bill says write a python script instead


  private Response handleServerError(Exception e) {
    log.error("Server side error", e);
    return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
        .entity(e.getMessage()).type(MediaType.TEXT_PLAIN_TYPE).build();
  }

  @GET
  @ApiOperation(value = "List objects", response = Object.class, responseContainer = "List")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
  public Response listObjects(
      @ApiParam(required = false) @QueryParam("bucketName") String bucketName) {

    try {
      return Response.status(Response.Status.OK).entity(
          new GenericEntity<List<Object>>(repoService.listObjects(bucketName)) {
          }).build();
    } catch (ClassNotFoundException e) {
      return Response.status(Response.Status.NOT_FOUND)
          .entity("Bucket not found").type(MediaType.TEXT_PLAIN_TYPE).build();
    } catch (RepoException e) {
      return Response.status(Response.Status.BAD_REQUEST)
          .entity(e.getMessage()).type(MediaType.TEXT_PLAIN_TYPE).build();
    } catch (SQLException e) {
      return handleServerError(e);
    }

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
  ) {

    Object object;

    try {
      object = repoService.getObject(bucketName, key, version);
    } catch (ClassNotFoundException e) {
      return Response.status(Response.Status.NOT_FOUND)
          .entity("Object not found: " + key).type(MediaType.TEXT_PLAIN_TYPE).build();
    } catch (SQLException e) {
      return handleServerError(e);
    }

    repoInfoService.incrementReadCount();

    // if they want the metadata

    if (fetchMetadata != null && fetchMetadata) {
      try {
        object.versions = repoService.getObjectVersions(object);
        return Response.status(Response.Status.OK).entity(object).build();
      } catch (SQLException e) {
        return handleServerError(e);
      }
    }

    // if they want redirect URLs

    if ( requestXProxy != null && requestXProxy.equals(REPROXY_HEADER_FILE) && repoService.serverSupportsReproxy()) {

      try {
        return Response.status(Response.Status.OK).header(REPROXY_HEADER_URL,
            REPROXY_URL_JOINER.join(repoService.getObjectReproxy(object)))
            .header(REPROXY_HEADER_CACHE_FOR, REPROXY_CACHE_FOR_HEADER).build();
      } catch (Exception e) {
        return handleServerError(e);
      }
    }

    // else assume they want the binary data

    try {

      String exportFileName = repoService.getObjectExportFileName(object);
      String contentType = repoService.getObjectContentType(object);
      InputStream is = repoService.getObjectInputStream(object);

      return Response.ok(is, contentType)
          .header("Content-Disposition", "inline; filename=" + exportFileName).build();

      // post: container will close this input stream

    } catch (Exception e) {
      return handleServerError(e);
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
                         @ApiParam(required = true) @QueryParam("version") Integer version) {

    if (key == null)
      return Response.status(Response.Status.BAD_REQUEST)
          .entity("No key entered").type(MediaType.TEXT_PLAIN).build();

    if (version == null)
      return Response.status(Response.Status.BAD_REQUEST)
          .entity("No version entered").type(MediaType.TEXT_PLAIN).build();

    try {
      repoService.deleteObject(bucketName, key, version);
      return Response.status(Response.Status.OK)
          .entity(key + " version " + version + " deleted").type(MediaType.TEXT_PLAIN).build();
    } catch (ClassNotFoundException e) {
      return Response.status(Response.Status.NOT_FOUND)
          .entity("Object not found: " + key).type(MediaType.TEXT_PLAIN_TYPE).build();
    } catch (SQLException e) {
      return handleServerError(e);
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

    Object existingObject;

    try {
      existingObject = repoService.getObject(bucketName, key, null);
    } catch (ClassNotFoundException e) {
      existingObject = null;
    } catch (SQLException e) {
      return handleServerError(e);
    }

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
  }

  private Response create(String key,
                          String bucketName,
                          String contentType,
                          String downloadName,
                          Timestamp timestamp,
                          InputStream uploadedInputStream) {

    if (uploadedInputStream == null)
      return Response.status(Response.Status.BAD_REQUEST)
          .entity("A file must be specified for uploading.").type(MediaType.TEXT_PLAIN).build();

    try {
      return Response.status(Response.Status.CREATED).entity(repoService.createNewObject(key, bucketName, contentType, downloadName, timestamp, uploadedInputStream)).build();
    } catch (RepoException e) {
      return Response.status(Response.Status.BAD_REQUEST)
          .entity(e.getMessage()).type(MediaType.TEXT_PLAIN_TYPE).build();
    } catch (Exception e) {
      return handleServerError(e);
    }

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

    if (object == null)
      return Response.status(Response.Status.NOT_ACCEPTABLE)
          .entity("Attempting to create a new version of an non-existing object.").type(MediaType.TEXT_PLAIN).build();

    try {
      return Response.status(Response.Status.CREATED).entity(repoService.updateObject(bucketName, contentType, downloadName, timestamp, uploadedInputStream, object)).build();
    } catch (RepoException e) {
      return Response.status(Response.Status.BAD_REQUEST)
          .entity(e.getMessage()).type(MediaType.TEXT_PLAIN_TYPE).build();
    } catch (Exception e) {
      return handleServerError(e);
    }

  }

}
