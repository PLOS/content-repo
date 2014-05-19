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

import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import com.wordnik.swagger.annotations.ApiResponse;
import com.wordnik.swagger.annotations.ApiResponses;
import org.apache.http.HttpStatus;
import org.plos.repo.models.Bucket;
import org.plos.repo.service.ObjectStore;
import org.plos.repo.service.SqlService;

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
@Api(value="/buckets")
public class BucketController {

  @Inject
  private ObjectStore objectStore;

  @Inject
  private SqlService sqlService;

  @GET
  @ApiOperation(value = "List buckets", response = Bucket.class, responseContainer = "List")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
  public Response list() throws Exception {
    return Response.status(Response.Status.OK).entity(
        new GenericEntity<List<Bucket>>(sqlService.listBuckets()){}).build();
  }

  @POST
  @ApiOperation(value = "Create a bucket")
  @ApiResponses(value = {
    @ApiResponse(code = HttpStatus.SC_CONFLICT, message = "The bucket was unable to be created (see response text for more details)"),
    @ApiResponse(code = HttpStatus.SC_PRECONDITION_FAILED, message = "Error in bucket name")
  })
  public Response create(@ApiParam(required = true) @FormParam("name") String name) {

    if (sqlService.getBucketId(name) != null)
      return Response.status(Response.Status.CONFLICT)
          .entity("Bucket already exists").type(MediaType.TEXT_PLAIN).build();

    if (!ObjectStore.isValidFileName(name))
      return Response.status(Response.Status.PRECONDITION_FAILED)
          .entity("Unable to create bucket. Name contains illegal characters: " + name).type(MediaType.TEXT_PLAIN).build();

    Bucket bucket = new Bucket(null, name);

    if (!objectStore.createBucket(bucket))
      return Response.status(Response.Status.CONFLICT)
          .entity("Unable to create bucket " + name).type(MediaType.TEXT_PLAIN).build();

    if (!sqlService.insertBucket(bucket)) {
      objectStore.deleteBucket(bucket);
      return Response.status(Response.Status.CONFLICT)
          .entity("Unable to create bucket " + name).type(MediaType.TEXT_PLAIN).build();
    }

    return Response.status(Response.Status.CREATED)
        .entity("Created bucket " + name).type(MediaType.TEXT_PLAIN).build();

  }

  @DELETE
  @Path("/{name}")
  @ApiOperation(value = "Delete a bucket")
  @ApiResponses(value = {
    @ApiResponse(code = HttpStatus.SC_NOT_MODIFIED, message = "The bucket was unable to be deleted (see response text for more details)")
  })
  public Response delete(@PathParam("name") String name) {

    // NOTE: it is hard to delete buckets since their objects never get completely removed

    if (sqlService.getBucketId(name) == null)
      return Response.status(Response.Status.NOT_MODIFIED)
          .entity("Cannot delete bucket. Bucket not found.").type(MediaType.TEXT_PLAIN).build();

    if (sqlService.listObjectsInBucket(name).size() != 0)
      return Response.status(Response.Status.NOT_MODIFIED)
          .entity("Cannot delete bucket " + name + " because it contains objects.").type(MediaType.TEXT_PLAIN).build();

    Bucket bucket = new Bucket(null, name);

    if (!objectStore.deleteBucket(bucket))
      return Response.status(Response.Status.NOT_MODIFIED)
          .entity("There was a problem removing the bucket").type(MediaType.TEXT_PLAIN).build();

    if (sqlService.deleteBucket(name) > 0)
      return Response.status(Response.Status.OK)
          .entity("Bucket " + name + " deleted.").type(MediaType.TEXT_PLAIN).build();

    return Response.status(Response.Status.NOT_MODIFIED).type(MediaType.TEXT_PLAIN)
        .entity("No buckets deleted.").build();

  }

}
