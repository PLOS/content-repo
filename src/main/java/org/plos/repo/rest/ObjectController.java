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
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.plos.repo.models.Object;
import org.plos.repo.service.RepoException;
import org.plos.repo.service.RepoInfoService;
import org.plos.repo.service.RepoService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.GenericEntity;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.InputStream;
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

  // default page size = number of objects returned when no limit= parameter supplied.
  private static final Integer DEFAULT_PAGE_SIZE = 1000;

  // maximum allowed value of page size, i.e., limit= parameter
  private static final Integer MAX_PAGE_SIZE = 10000;

  private static final DateTimeFormatter dateTimeFormatter = DateTimeFormat.forPattern("EEE, dd MMM yyyy HH:mm:ss z");

  @Inject
  private RepoService repoService;

  @Inject
  private RepoInfoService repoInfoService;


  // TODO: check at startup that db is in sync with objectStore ? bill says write a python script instead


  public static Response handleError(RepoException e) {

    switch (e.getType()) {

      case ClientError:
        return Response.status(Response.Status.BAD_REQUEST)
            .entity(e.getMessage()).type(MediaType.TEXT_PLAIN_TYPE).build();

      case ItemNotFound:
        return Response.status(Response.Status.NOT_FOUND)
            .entity(e.getMessage()).type(MediaType.TEXT_PLAIN_TYPE).build();

      default:  // ServerError
        log.error(e.getType().toString(), e);
        return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
            .entity(e.getMessage()).type(MediaType.TEXT_PLAIN_TYPE).build();
    }

  }


  @GET
  @ApiOperation(value = "List objects", response = Object.class, responseContainer = "List")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
  public Response listObjects(
      @ApiParam(required = false) @QueryParam("bucketName") String bucketName,
      @ApiParam(required = false) @QueryParam("offset") Integer offset,
      @ApiParam(required = false) @QueryParam("limit") Integer limit,
      @ApiParam(required = false) @DefaultValue("false") @QueryParam("includeDeleted") boolean includeDeleted) {

    if (offset == null)
      offset = 0;
    if (limit == null)
      limit = DEFAULT_PAGE_SIZE;

    if (offset < 0)
      return Response.status(Response.Status.BAD_REQUEST)
          .entity("Invalid offset").type(MediaType.TEXT_PLAIN).build();

    if (limit <= 0 || limit > MAX_PAGE_SIZE)
      return Response.status(Response.Status.BAD_REQUEST)
          .entity("Invalid limit").type(MediaType.TEXT_PLAIN).build();

    try {
      return Response.status(Response.Status.OK).entity(
          new GenericEntity<List<Object>>(repoService.listObjects(bucketName, offset, limit, includeDeleted)) {
          }).build();
    } catch (RepoException e) {
      return handleError(e);
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
                       @HeaderParam("X-Proxy-Capabilities") String requestXProxy,
                       @HeaderParam("If-Modified-Since") String ifModifiedSinceStr
  ) {

    Object object;

    try {
      object = repoService.getObject(bucketName, key, version);
    } catch (RepoException e) {
      return handleError(e);
    }

    DateTime objDateTime = new DateTime(object.timestamp);
    String httpDateStr = dateTimeFormatter.print(objDateTime);
    boolean notModifiedSince = false;

    if (ifModifiedSinceStr != null)
      objDateTime.isBefore(dateTimeFormatter.parseDateTime(ifModifiedSinceStr));

    repoInfoService.incrementReadCount();

    // if they want the metadata

    if (fetchMetadata != null && fetchMetadata) {
      try {
        object.versions = repoService.getObjectVersions(object);
        return Response.status(Response.Status.OK)
            .header(HttpHeaders.LAST_MODIFIED, httpDateStr)
            .entity(object).build();
      } catch (RepoException e) {
        return handleError(e);
      }
    }


    // if they want redirect URLs

    if (requestXProxy != null && requestXProxy.equals(REPROXY_HEADER_FILE) && repoService.serverSupportsReproxy()) {

      try {
        Response.Status status = Response.Status.OK;

        if (notModifiedSince)
          status = Response.Status.NOT_MODIFIED;

        return Response.status(status)
            .header(HttpHeaders.LAST_MODIFIED, httpDateStr)
            .header(REPROXY_HEADER_URL, REPROXY_URL_JOINER.join(repoService.getObjectReproxy(object)))
            .header(REPROXY_HEADER_CACHE_FOR, REPROXY_CACHE_FOR_HEADER)
            .build();
      } catch (RepoException e) {
        return handleError(e);
      }
    }


    // else assume they want the binary data

    try {

      if (notModifiedSince)
        return Response.notModified().header(HttpHeaders.LAST_MODIFIED, httpDateStr).build();

      String exportFileName = repoService.getObjectExportFileName(object);
      String contentType = repoService.getObjectContentType(object);
      InputStream is = repoService.getObjectInputStream(object);

      return Response.ok(is, contentType)
          .header(HttpHeaders.LAST_MODIFIED, httpDateStr)
          .header(HttpHeaders.CONTENT_DISPOSITION, "inline; filename=" + exportFileName).build();

      // the container closes this input stream

    } catch (RepoException e) {
      return handleError(e);
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
    } catch (RepoException e) {
      return handleError(e);
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
  ) {

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

    try {

      RepoService.CreateMethod method = null;

      try {
        method = RepoService.CreateMethod.valueOf(create.toUpperCase());
      } catch (IllegalArgumentException e) {
        throw new RepoException(RepoException.Type.ClientError, e);
      }

      return Response.status(Response.Status.CREATED).entity(repoService.createObject(method, key, bucketName, contentType, downloadName, timestamp, uploadedInputStream)).build();

    } catch (RepoException e) {
      return handleError(e);
    }

  }

}
