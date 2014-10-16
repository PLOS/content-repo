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
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.wordnik.swagger.annotations.*;
import org.apache.http.HttpStatus;
import org.glassfish.jersey.media.multipart.FormDataParam;
import org.plos.repo.models.RepoError;
import org.plos.repo.models.RepoObject;
import org.plos.repo.models.input.ElementFilter;
import org.plos.repo.models.output.RepoObjectOutput;
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
import java.text.ParseException;
import java.text.SimpleDateFormat;
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

  private static final String RFC1123_DATE_TIME_FORMAT =  "EEE, dd MMM yyyy HH:mm:ss z";

  @Inject
  private RepoService repoService;

  @Inject
  private RepoInfoService repoInfoService;


  public static Response handleError(RepoException e) {

    Response.Status status = Response.Status.BAD_REQUEST;

    switch (e.getType()) {

      case BucketNotFound:
      case ObjectNotFound:
      case CollectionNotFound:
      case ObjectCollectionNotFound:
        status = Response.Status.NOT_FOUND;
        break;

      case ServerError:
        status = Response.Status.INTERNAL_SERVER_ERROR;
        log.error(e.getType().toString(), e);
        break;
    }

    return Response.status(status).entity(new RepoError(e)).build();

  }


  @GET
  @ApiOperation(value = "List objects", response = RepoObjectOutput.class, responseContainer = "List")
  @ApiResponses(value = {
      @ApiResponse(code = HttpStatus.SC_OK, message = "Success"),
      @ApiResponse(code = HttpStatus.SC_NOT_FOUND, message = "Bucket not found"),
      @ApiResponse(code = HttpStatus.SC_BAD_REQUEST, message = "Bad request (see message)"),
      @ApiResponse(code = HttpStatus.SC_INTERNAL_SERVER_ERROR, message = "Server error")
  })
  @Produces({MediaType.APPLICATION_JSON})
  public Response listObjects(
      @ApiParam(required = false) @QueryParam("bucketName") String bucketName,
      @ApiParam(required = false) @QueryParam("offset") Integer offset,
      @ApiParam(required = false) @QueryParam("limit") Integer limit,
      @ApiParam(required = false) @DefaultValue("false") @QueryParam("includeDeleted") boolean includeDeleted,
      @ApiParam(required = false) @QueryParam("tag") String tag) {

    try {

      List<RepoObject> repoObjects = repoService.listObjects(bucketName, offset, limit, includeDeleted, tag);
      List<RepoObjectOutput> outputObjects = Lists.newArrayList(Iterables.transform(repoObjects, RepoObjectOutput.typeFunction()));

      return Response.status(Response.Status.OK).entity(
          new GenericEntity<List<RepoObjectOutput>>(
              outputObjects
          ) {}).build();
    } catch (RepoException e) {
      return handleError(e);
    }

  }

  @GET @Path("/meta/{bucketName}")
  @ApiOperation(value = "Fetch info about an object and its versions", response = RepoObjectOutput.class)
  @Produces({MediaType.APPLICATION_JSON})
  public Response readMetadata(
      @ApiParam(required = true) @PathParam("bucketName") String bucketName,
      @ApiParam(required = true) @QueryParam("key") String key,
      @ApiParam("elementFilter") @BeanParam ElementFilter elementFilter) {

    try {

      RepoObject repoObject = repoService.getObject(bucketName, key, elementFilter);

      RepoObjectOutput outputObject = new RepoObjectOutput(repoObject);

      return Response.status(Response.Status.OK)
          .lastModified(repoObject.getTimestamp())
          .entity(outputObject).build();
    } catch (RepoException e) {
      return handleError(e);
    }

  }

  @GET @Path("/{bucketName}")
  @ApiOperation(value = "Fetch an object or its metadata", response = RepoObjectOutput.class)
  @Produces({MediaType.APPLICATION_JSON})
  public Response read(@ApiParam(required = true) @PathParam("bucketName") String bucketName,
                       @ApiParam(required = true) @QueryParam("key") String key,
                       @ApiParam("elementFilter") @BeanParam ElementFilter elementFilter,
                       @QueryParam("fetchMetadata") Boolean fetchMetadata,  // TODO: deprecate this somehow
                       @ApiParam(value = "If set to 'reproxy-file' then it will attempt to return a header representing a redirected object URL")
                       @HeaderParam("X-Proxy-Capabilities") String requestXProxy,
                       @HeaderParam("If-Modified-Since") String ifModifiedSinceStr
  ) {

    RepoObject repoObject;

    boolean notModifiedSince = false;

    try {
      repoObject = repoService.getObject(bucketName, key, elementFilter);

      if (ifModifiedSinceStr != null) {

        Date ifModifiedSince = new SimpleDateFormat(RFC1123_DATE_TIME_FORMAT).parse(ifModifiedSinceStr);
        notModifiedSince = repoObject.getTimestamp().compareTo(ifModifiedSince) <= 0;
      }

    } catch (ParseException e) {
      return handleError(new RepoException(RepoException.Type.CouldNotParseTimestamp));
    } catch (RepoException e) {
      return handleError(e);
    }

    repoInfoService.incrementReadCount();

    // if they want the metadata

    if (fetchMetadata != null && fetchMetadata) {
        RepoObjectOutput outputObject = new RepoObjectOutput(repoObject);
        return Response.status(Response.Status.OK)
            .lastModified(repoObject.getTimestamp())
            .entity(outputObject).build();
    }


    // if they want redirect URLs

    if (requestXProxy != null && requestXProxy.equals(REPROXY_HEADER_FILE) && repoService.serverSupportsReproxy()) {

      try {
        Response.Status status = Response.Status.OK;

        if (notModifiedSince)
          status = Response.Status.NOT_MODIFIED;

        return Response.status(status)
            .lastModified(repoObject.getTimestamp())
            .header(REPROXY_HEADER_URL, REPROXY_URL_JOINER.join(repoService.getObjectReproxy(repoObject)))
            .header(REPROXY_HEADER_CACHE_FOR, REPROXY_CACHE_FOR_HEADER)
            .build();
      } catch (RepoException e) {
        return handleError(e);
      }
    }


    // else assume they want the binary data

    try {

      if (notModifiedSince)
        return Response.notModified().lastModified(repoObject.getTimestamp()).build();

      String exportFileName = repoService.getObjectExportFileName(repoObject);
      String contentType = repoService.getObjectContentType(repoObject);
      InputStream is = repoService.getObjectInputStream(repoObject);

      return Response.ok(is, contentType)
          .lastModified(repoObject.getTimestamp())
          .header(HttpHeaders.CONTENT_DISPOSITION, "inline; filename=" + exportFileName).build();

      // the container closes this input stream

    } catch (RepoException e) {
      return handleError(e);
    }

  }

  @GET @Path("/versions/{bucketName}")
  @ApiOperation(value = "Fetch all the object versions", response = RepoObjectOutput.class, responseContainer = "List")
  @Produces({MediaType.APPLICATION_JSON})
  public Response getVersions(@ApiParam(required = true) @PathParam("bucketName") String bucketName,
                       @ApiParam(required = true) @QueryParam("key") String key) {

    try {

      List<RepoObject> repoObjects = repoService.getObjectVersions(bucketName, key);

      List<RepoObjectOutput> outputObjects = Lists.newArrayList(Iterables.transform(repoObjects, RepoObjectOutput.typeFunction()));

      return Response.status(Response.Status.OK).entity(
          new GenericEntity<List<RepoObjectOutput>>(
              outputObjects
          ) {}).build();
    } catch (RepoException e) {
      return handleError(e);
    }

  }

  @DELETE
  @Path("/{bucketName}")
  @ApiOperation(value = "Delete an object")
  @ApiResponses(value = {
      @ApiResponse(code = HttpStatus.SC_OK, message = "Object successfully deleted"),
      @ApiResponse(code = HttpStatus.SC_NOT_FOUND, message = "The object was not found"),
      @ApiResponse(code = HttpStatus.SC_BAD_REQUEST, message = "The object was unable to be deleted (see response text for more details)"),
      @ApiResponse(code = HttpStatus.SC_INTERNAL_SERVER_ERROR, message = "Server error")
  })
  public Response delete(
      @ApiParam(required = true) @PathParam("bucketName") String bucketName,
      @ApiParam(required = true) @QueryParam("key") String key,
      @ApiParam("elementFilter") @BeanParam ElementFilter elementFilter
  ) {

    try {
      repoService.deleteObject(bucketName, key, elementFilter);
      return Response.status(Response.Status.OK).build();
    } catch (RepoException e) {
      return handleError(e);
    }

  }

  @POST
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  @ApiOperation(value = "Create a new object or a new version of an existing object",
      notes = "Set the create field to 'new' object if the object you are inserting is not already in the repo. If you want to create a " +
          "new version of an existing object set create to 'version'. Setting create to 'auto' automagically determines if the object " +
          "should be new or versioned. However 'auto' should only be used by the ambra-file-store. In addition you may optionally specify " +
          "a timestamp for object creation time. This feature is for migrating from an existing content store. Note that the timestamp must " +
          "conform to this format: yyyy-[m]m-[d]d hh:mm:ss[.f...]")
  @Produces({MediaType.APPLICATION_JSON})
  @ApiResponses(value = {
      @ApiResponse(code = HttpStatus.SC_CREATED, message = "Object successfully created", response = RepoObjectOutput.class),
      @ApiResponse(code = HttpStatus.SC_NOT_FOUND, message = "The object not found"),
      @ApiResponse(code = HttpStatus.SC_BAD_REQUEST, message = "The object was unable to be created (see response text for more details)"),
      @ApiResponse(code = HttpStatus.SC_INTERNAL_SERVER_ERROR, message = "Server error")
  })
  public Response createOrUpdate(
      @ApiParam(required = true) @FormDataParam("key") String key,
      @ApiParam(required = true) @FormDataParam("bucketName") String bucketName,
      @ApiParam(value = "MIME type") @FormDataParam("contentType") String contentType,
      @ApiParam(value = "name of file when downloaded", required = false) @FormDataParam("downloadName") String downloadName,
      @ApiParam(value = "creation method", allowableValues = "new,version,auto", defaultValue = "new",
          required = true) @FormDataParam("create") String create,
      @ApiParam(value = "last modification time", required = false) @FormDataParam("timestamp") String timestamp,
      @ApiParam(value = "creation time", required = false) @FormDataParam("creationDateTime") String creationDateTime,
      @ApiParam(value = "creation time", required = false) @FormDataParam("tag") String tag,
      @ApiParam(required = false) @FormDataParam("file") InputStream uploadedInputStream
  ) {

    try {

      RepoService.CreateMethod method;

      if (create == null)
        throw new RepoException(RepoException.Type.NoCreationMethodEntered);

      try {
        method = RepoService.CreateMethod.valueOf(create.toUpperCase());
      } catch (IllegalArgumentException e) {
        throw new RepoException(RepoException.Type.InvalidCreationMethod);
      }

      Timestamp defaultTimeStamp = new Timestamp(new Date().getTime());

      Timestamp creationDateTimestamp = getValidateTimestamp(creationDateTime, RepoException.Type.CouldNotParseCreationDate, defaultTimeStamp);
      Timestamp lastModifiedDateTime = getValidateTimestamp(timestamp, RepoException.Type.CouldNotParseTimestamp, creationDateTimestamp);

      repoInfoService.incrementWriteCount();

      RepoObject repoObject = repoService.createObject(method, key, bucketName, contentType, downloadName, lastModifiedDateTime, uploadedInputStream, creationDateTimestamp, tag);
      RepoObjectOutput outputObject = new RepoObjectOutput(repoObject);

      return Response.status(Response.Status.CREATED).entity(
          outputObject).build();

    } catch (RepoException e) {
      return handleError(e);
    }

  }

  private Timestamp getValidateTimestamp(String timestampString, RepoException.Type errorType, Timestamp defaultTimestamp) throws RepoException{
    if (timestampString != null) {
      try {
        return Timestamp.valueOf(timestampString);
      } catch (IllegalArgumentException e) {
        throw new RepoException(errorType);
      }
    }
    return defaultTimestamp;
  }

}
