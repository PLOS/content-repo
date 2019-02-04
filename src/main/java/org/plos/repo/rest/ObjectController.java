/*
 * Copyright (c) 2014-2019 Public Library of Science
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
 * THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 */

package org.plos.repo.rest;

import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import com.wordnik.swagger.annotations.ApiResponse;
import com.wordnik.swagger.annotations.ApiResponses;
import org.apache.http.HttpStatus;
import org.plos.repo.models.RepoError;
import org.plos.repo.models.RepoObject;
import org.plos.repo.models.input.ElementFilter;
import org.plos.repo.models.input.InputRepoObject;
import org.plos.repo.models.output.RepoObjectOutput;
import org.plos.repo.service.RepoException;
import org.plos.repo.service.RepoInfoService;
import org.plos.repo.service.RepoService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.ws.rs.BeanParam;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
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
@Api(value = "/objects")
public class ObjectController {

  private static final Logger log = LoggerFactory.getLogger(ObjectController.class);

  private static final Joiner REPROXY_URL_JOINER = Joiner.on(' ');

  private static final int REPROXY_CACHE_FOR_VALUE = 6 * 60 * 60; // TODO: Make configurable

  private static final String REPROXY_CACHE_FOR_HEADER =
      REPROXY_CACHE_FOR_VALUE + "; Last-Modified Content-Type Content-Disposition";

  private static final String REPROXY_HEADER_URL = "X-Reproxy-URL";

  private static final String REPROXY_HEADER_CACHE_FOR = "X-Reproxy-Cache-For";

  private static final String REPROXY_HEADER_FILE = "reproxy-file";

  private static final String RFC1123_DATE_TIME_FORMAT = "EEE, dd MMM yyyy HH:mm:ss z";

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

      case NoFileEntered:
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
      @ApiParam(required = true) @QueryParam("bucketName") String bucketName,
      @ApiParam(required = false) @QueryParam("offset") Integer offset,
      @ApiParam(required = false) @QueryParam("limit") Integer limit,
      @ApiParam(required = false) @DefaultValue("false") @QueryParam("includeDeleted") boolean includeDeleted,
      @ApiParam(required = false) @DefaultValue("false") @QueryParam("includePurged") boolean includePurged,
      @ApiParam(required = false) @QueryParam("tag") String tag) {
    try {
      List<RepoObject> repoObjects = repoService.listObjects(bucketName, offset, limit, includeDeleted, includePurged, tag);
      List<RepoObjectOutput> outputObjects = Lists.newArrayList(Iterables.transform(repoObjects, RepoObjectOutput.typeFunction()));

      return Response.status(Response.Status.OK).entity(
          new GenericEntity<List<RepoObjectOutput>>(
              outputObjects
          ) {
          }).build();
    } catch (RepoException e) {
      return handleError(e);
    }
  }

  @GET
  @Path("/meta/{bucketName}")
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

  @GET
  @Path("/{bucketName}")
  @ApiOperation(value = "Fetch an object or its metadata", response = RepoObjectOutput.class)
  @Produces({MediaType.APPLICATION_JSON})
  public Response read(@ApiParam(required = true) @PathParam("bucketName") String bucketName,
                       @ApiParam(required = true) @QueryParam("key") String key,
                       @ApiParam("elementFilter") @BeanParam ElementFilter elementFilter,
                       @QueryParam("fetchMetadata") boolean fetchMetadata,  // TODO: deprecate this somehow
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

    if (fetchMetadata) {
      RepoObjectOutput outputObject = new RepoObjectOutput(repoObject);
      return Response.status(Response.Status.OK)
          .lastModified(repoObject.getTimestamp())
          .entity(outputObject).build();
    }


    // if they want redirect URLs

    if (requestXProxy != null && requestXProxy.equals(REPROXY_HEADER_FILE) && repoService.serverSupportsReproxy()) {
      try {
        Response.Status status = Response.Status.OK;

        if (notModifiedSince) {
          status = Response.Status.NOT_MODIFIED;
        }

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
      if (notModifiedSince) {
        return Response.notModified().lastModified(repoObject.getTimestamp()).build();
      }

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

  @GET
  @Path("/versions/{bucketName}")
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
          ) {
          }).build();
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
      @ApiParam(required = false) @DefaultValue("false") @QueryParam("purge") boolean purge,
      @ApiParam("elementFilter") @BeanParam ElementFilter elementFilter
  ) {
    try {
      repoService.deleteObject(bucketName, key, purge, elementFilter);
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
  public Response createOrUpdate(@BeanParam InputRepoObject inputRepoObject) {
    try {
      RepoService.CreateMethod method;

      if (inputRepoObject.getCreate() == null) {
        throw new RepoException(RepoException.Type.NoCreationMethodEntered);
      }

      try {
        method = RepoService.CreateMethod.valueOf(inputRepoObject.getCreate().toUpperCase());
      } catch (IllegalArgumentException e) {
        throw new RepoException(RepoException.Type.InvalidCreationMethod);
      }

      repoInfoService.incrementWriteCount();

      RepoObject repoObject = repoService.createObject(method, inputRepoObject);
      RepoObjectOutput outputObject = new RepoObjectOutput(repoObject);

      return Response.status(Response.Status.CREATED).entity(
          outputObject).build();
    } catch (RepoException e) {
      return handleError(e);
    }
  }

  private Timestamp getValidateTimestamp(String timestampString, RepoException.Type errorType, Timestamp defaultTimestamp) throws RepoException {
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
