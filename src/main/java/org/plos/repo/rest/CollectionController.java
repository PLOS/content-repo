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

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import com.wordnik.swagger.annotations.ApiResponse;
import com.wordnik.swagger.annotations.ApiResponses;
import org.apache.http.HttpStatus;
import org.plos.repo.models.RepoCollection;
import org.plos.repo.models.input.ElementFilter;
import org.plos.repo.models.input.InputCollection;
import org.plos.repo.models.output.RepoCollectionOutput;
import org.plos.repo.service.CollectionRepoService;
import org.plos.repo.service.RepoException;
import org.plos.repo.service.RepoInfoService;
import org.plos.repo.service.RepoService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.ws.rs.BeanParam;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.GenericEntity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;


@Path("/collections")
@Api(value = "/collections")
public class CollectionController {

  private static final Logger log = LoggerFactory.getLogger(CollectionController.class);

  @Inject
  private CollectionRepoService collectionRepoService;

  @Inject
  private RepoInfoService repoInfoService;


  @GET
  @ApiOperation(value = "List collections", response = RepoCollectionOutput.class, responseContainer = "List")
  @ApiResponses(value = {
      @ApiResponse(code = HttpStatus.SC_OK, message = "Success"),
      @ApiResponse(code = HttpStatus.SC_NOT_FOUND, message = "Bucket not found / Collection not found"),
      @ApiResponse(code = HttpStatus.SC_BAD_REQUEST, message = "Bad request (see message)"),
      @ApiResponse(code = HttpStatus.SC_INTERNAL_SERVER_ERROR, message = "Server error")
  })
  @Produces({MediaType.APPLICATION_JSON})
  public Response listCollections(
      @ApiParam(required = true) @QueryParam("bucketName") String bucketName,
      @ApiParam(required = false) @QueryParam("offset") Integer offset,
      @ApiParam(required = false) @QueryParam("limit") Integer limit,
      @ApiParam(required = false) @DefaultValue("false") @QueryParam("includeDeleted") boolean includeDeleted,
      @ApiParam(required = false) @QueryParam("tag") String tag) {

    try {

      List<RepoCollection> repoCollections = collectionRepoService.listCollections(bucketName, offset, limit, includeDeleted, tag);
      List<RepoCollectionOutput> outputCollections = Lists.newArrayList(Iterables.transform(repoCollections, RepoCollectionOutput.typeFunction()));

      return Response.status(Response.Status.OK)
          .entity(new GenericEntity<List<RepoCollectionOutput>>(outputCollections) {
          })
          .build();

    } catch (RepoException e) {

      return ObjectController.handleError(e);
    }

  }

  @GET
  @Path("/{bucketName}")
  @ApiOperation(value = "Fetch a collection", response = RepoCollectionOutput.class)
  @ApiResponses(value = {
      @ApiResponse(code = HttpStatus.SC_OK, message = "Success"),
      @ApiResponse(code = HttpStatus.SC_NOT_FOUND, message = "Bucket not found"),
      @ApiResponse(code = HttpStatus.SC_BAD_REQUEST, message = "Bad request (see message)"),
      @ApiResponse(code = HttpStatus.SC_INTERNAL_SERVER_ERROR, message = "Server error")
  })
  @Produces({MediaType.APPLICATION_JSON})
  public Response getCollection(
      @ApiParam(required = true) @PathParam("bucketName") String bucketName,
      @ApiParam(required = true) @QueryParam("key") String key,
      @ApiParam("collectionFilter") @BeanParam ElementFilter elementFilter) {

    try {

      RepoCollection repoCollection = collectionRepoService.getCollection(bucketName, key, elementFilter);

      RepoCollectionOutput outputCollection = new RepoCollectionOutput(repoCollection);

      return Response.status(Response.Status.OK)
          .lastModified(repoCollection.getTimestamp())
          .entity(outputCollection).build();
    } catch (RepoException e) {
      return ObjectController.handleError(e);
    }

  }

  @GET
  @Path("/versions/{bucketName}")
  @ApiOperation(value = "Fetch all the collection versions", response = RepoCollectionOutput.class, responseContainer = "List")
  @ApiResponses(value = {
      @ApiResponse(code = HttpStatus.SC_OK, message = "Success"),
      @ApiResponse(code = HttpStatus.SC_NOT_FOUND, message = "Bucket not found"),
      @ApiResponse(code = HttpStatus.SC_BAD_REQUEST, message = "Bad request (see message)"),
      @ApiResponse(code = HttpStatus.SC_INTERNAL_SERVER_ERROR, message = "Server error")
  })
  @Produces({MediaType.APPLICATION_JSON})
  public Response getCollectionVersions(
      @ApiParam(required = true) @PathParam("bucketName") String bucketName,
      @ApiParam(required = true) @QueryParam("key") String key) {

    try {

      List<RepoCollection> repoCollections = collectionRepoService.getCollectionVersions(bucketName, key);

      List<RepoCollectionOutput> outputCollections = Lists.newArrayList(Iterables.transform(repoCollections, RepoCollectionOutput.typeFunction()));

      return Response.status(Response.Status.OK)
          .entity(new GenericEntity<List<RepoCollectionOutput>>(outputCollections) {
          })
          .build();
    } catch (RepoException e) {
      return ObjectController.handleError(e);
    }

  }

  @DELETE
  @Path("/{bucketName}")
  @ApiOperation(value = "Delete a collection")
  @ApiResponses(value = {
      @ApiResponse(code = HttpStatus.SC_OK, message = "Collection successfully deleted"),
      @ApiResponse(code = HttpStatus.SC_NOT_FOUND, message = "The collection was not found"),
      @ApiResponse(code = HttpStatus.SC_BAD_REQUEST, message = "The collection was unable to be deleted (see response text for more details)"),
      @ApiResponse(code = HttpStatus.SC_INTERNAL_SERVER_ERROR, message = "Server error")
  })
  public Response delete(
      @ApiParam(required = true) @PathParam("bucketName") String bucketName,
      @ApiParam(required = true) @QueryParam("key") String key,
      @ApiParam("collectionFilter") @BeanParam ElementFilter elementFilter) {

    try {
      collectionRepoService.deleteCollection(bucketName, key, elementFilter);
      return Response.status(Response.Status.OK).build();
    } catch (RepoException e) {
      return ObjectController.handleError(e);
    }

  }


  @POST
  @ApiOperation(value = "Create a new collection or a new version of an existing collection",
      notes = "Set the create field to 'new' collection if the collection you are inserting is not already in the repo. If you want to create a new version of an existing collection set create to 'version'. ")
  @Produces({MediaType.APPLICATION_JSON})
  @ApiResponses(value = {
      @ApiResponse(code = HttpStatus.SC_CREATED, message = "Collection successfully created"),
      @ApiResponse(code = HttpStatus.SC_NOT_FOUND, message = "The object not found"),
      @ApiResponse(code = HttpStatus.SC_BAD_REQUEST, message = "The collection was unable to be created (see response text for more details)"),
      @ApiResponse(code = HttpStatus.SC_INTERNAL_SERVER_ERROR, message = "Server error")
  })
  public Response createOrUpdateCollection(@ApiParam("collection") InputCollection inputCollection) {

    try {

      RepoService.CreateMethod method;

      if (inputCollection.getCreate() == null) {
        throw new RepoException(RepoException.Type.NoCreationMethodEntered);
      }

      try {
        method = RepoService.CreateMethod.valueOf(inputCollection.getCreate().toUpperCase());
      } catch (IllegalArgumentException e) {
        throw new RepoException(RepoException.Type.InvalidCreationMethod);
      }

      repoInfoService.incrementWriteCount();

      RepoCollection repoCollection = collectionRepoService.createCollection(method, inputCollection);

      RepoCollectionOutput outputCollection = new RepoCollectionOutput(repoCollection);

      return Response.status(Response.Status.CREATED).entity(outputCollection).build();

    } catch (RepoException e) {
      return ObjectController.handleError(e);
    }

  }


}
