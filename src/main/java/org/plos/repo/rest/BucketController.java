/*
 * Copyright (c) 2017 Public Library of Science
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

import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import com.wordnik.swagger.annotations.ApiResponse;
import com.wordnik.swagger.annotations.ApiResponses;
import org.apache.http.HttpStatus;
import org.plos.repo.models.Bucket;
import org.plos.repo.service.RepoException;
import org.plos.repo.service.RepoInfoService;
import org.plos.repo.service.RepoService;

import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.GenericEntity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;

@Path("/buckets")
@Api(value = "/buckets")
public class BucketController {

  @Inject
  private RepoService repoService;

  @Inject
  private RepoInfoService repoInfoService;

  @GET
  @ApiOperation(value = "List buckets", response = Bucket.class, responseContainer = "List")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
  public Response list() {
    try {
      return Response.status(Response.Status.OK).entity(
          new GenericEntity<List<Bucket>>(repoService.listBuckets()) {
          }).build();
    } catch (RepoException e) {
      return ObjectController.handleError(e);
    }
  }

  @GET
  @Path("/{bucketName}")
  @ApiOperation(value = "Info about the bucket", response = Bucket.class)
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
  public Response info(@PathParam("bucketName") String bucketName) {
    try {
      return Response.status(Response.Status.OK).entity(
          repoInfoService.bucketInfo(bucketName)
      ).build();
    } catch (RepoException e) {
      return ObjectController.handleError(e);
    }
  }

  @POST
  @ApiOperation(value = "Create a bucket")
  @ApiResponses(value = {
      @ApiResponse(code = HttpStatus.SC_BAD_REQUEST, message = "The bucket was unable to be created (see response text for more details)"),
      @ApiResponse(code = HttpStatus.SC_INTERNAL_SERVER_ERROR, message = "Server error")
  })
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
  public Response create(@ApiParam(required = true) @FormParam("name") String name,
                         @ApiParam(value = "creation time", required = false) @FormParam("creationDateTime") String creationDateTimeString) {
    try {
      return Response.status(Response.Status.CREATED).entity(
          repoService.createBucket(name, creationDateTimeString)
      ).build();
    } catch (RepoException e) {
      return ObjectController.handleError(e);
    }
  }

  @DELETE
  @Path("/{name}")
  @ApiOperation(value = "Delete a bucket")
  @ApiResponses(value = {
      @ApiResponse(code = HttpStatus.SC_NOT_FOUND, message = "The bucket was not found"),
      @ApiResponse(code = HttpStatus.SC_BAD_REQUEST, message = "The bucket was unable to be deleted (see response text for more details)"),
      @ApiResponse(code = HttpStatus.SC_INTERNAL_SERVER_ERROR, message = "Server error")
  })
  public Response delete(@PathParam("name") String name) {
    try {
      repoService.deleteBucket(name);
      return Response.status(Response.Status.OK).build();
    } catch (RepoException e) {
      return ObjectController.handleError(e);
    }
  }

}
