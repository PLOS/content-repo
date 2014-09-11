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

import com.wordnik.swagger.annotations.*;
import org.apache.http.HttpStatus;
import org.plos.repo.models.Collection;
import org.plos.repo.models.RepoError;
import org.plos.repo.models.SmallCollection;
import org.plos.repo.service.RepoException;
import org.plos.repo.service.RepoInfoService;
import org.plos.repo.service.RepoService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.GenericEntity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.sql.Timestamp;
import java.util.Date;
import java.util.List;


@Path("/collections")
@Api(value="/collections")
public class CollectionController {

    private static final Logger log = LoggerFactory.getLogger(CollectionController.class);

    @Inject
    private RepoService repoService;

    @Inject
    private RepoInfoService repoInfoService;


    @GET
    @ApiOperation(value = "List collections", response = Collection.class, responseContainer = "List")
    @ApiResponses(value = {
    @ApiResponse(code = HttpStatus.SC_OK, message = "Success"),
    @ApiResponse(code = HttpStatus.SC_NOT_FOUND, message = "Bucket not found"),
    @ApiResponse(code = HttpStatus.SC_BAD_REQUEST, message = "Bad request (see message)"),
    @ApiResponse(code = HttpStatus.SC_INTERNAL_SERVER_ERROR, message = "Server error")
    })
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    public Response listCollections(
            @ApiParam(required = false) @QueryParam("bucketName") String bucketName,
            @ApiParam(required = false) @QueryParam("offset") Integer offset,
            @ApiParam(required = false) @QueryParam("limit") Integer limit ) {

        try {
            return Response.status(Response.Status.OK).entity(
            new GenericEntity<List<Collection>>(
                    repoService.listCollections(bucketName, offset, limit)) {})
                    .build();

        } catch (RepoException e) {

            return ObjectController.handleError(e);
        }

    }

    @GET
    @Path("/{bucketName}")
    @ApiOperation(value = "Fetch a collection", response = Collection.class)
    @ApiResponses(value = {
            @ApiResponse(code = HttpStatus.SC_OK, message = "Success"),
            @ApiResponse(code = HttpStatus.SC_NOT_FOUND, message = "Bucket not found"),
            @ApiResponse(code = HttpStatus.SC_BAD_REQUEST, message = "Bad request (see message)"),
            @ApiResponse(code = HttpStatus.SC_INTERNAL_SERVER_ERROR, message = "Server error")
    })
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    public Response listCollections(
            @ApiParam(required = true) @PathParam("bucketName") String bucketName,
            @ApiParam(required = true) @QueryParam("key") String key,
            @QueryParam("version") Integer version) {

        try {

            Collection collection = repoService.getCollection(bucketName, key, version);

            collection.setVersions(repoService.getCollectionVersions(collection));
            return Response.status(Response.Status.OK)
                    .lastModified(collection.getTimestamp())
                    .entity(collection).build();
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
            @ApiParam(required = true) @QueryParam("version") Integer version) {

        try {
            repoService.deleteCollection(bucketName, key, version);
            return Response.status(Response.Status.OK).build();
        } catch (RepoException e) {
            return ObjectController.handleError(e);
        }

    }


    @POST
    @ApiOperation(value = "Create a new collection or a new version of an existing collection",
            notes = "Set the create field to 'new' collection if the collection you are inserting is not already in the repo. If you want to create a new version of an existing collection set create to 'version'. ")
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @ApiResponses(value = {
            @ApiResponse(code = HttpStatus.SC_CREATED, message = "Collection successfully created"),
            @ApiResponse(code = HttpStatus.SC_NOT_FOUND, message = "The object not found"),
            @ApiResponse(code = HttpStatus.SC_BAD_REQUEST, message = "The collection was unable to be created (see response text for more details)"),
            @ApiResponse(code = HttpStatus.SC_INTERNAL_SERVER_ERROR, message = "Server error")
    })
    public Response createOrUpdateCollection(@ApiParam("collection") SmallCollection smallCollection){

        try {

            RepoService.CreateMethod method;

            if (smallCollection.getCreate() == null)
                throw new RepoException(RepoException.Type.NoCreationMethodEntered);

            try {
                method = RepoService.CreateMethod.valueOf(smallCollection.getCreate().toUpperCase());
            } catch (IllegalArgumentException e) {
                throw new RepoException(RepoException.Type.InvalidCreationMethod);
            }

        smallCollection.setTimestamp(new Timestamp(new Date().getTime()));

        if (smallCollection.getTimestampString() != null) {
            try {
                smallCollection.setTimestamp(Timestamp.valueOf(smallCollection.getTimestampString()));
            } catch (IllegalArgumentException e) {
                throw new RepoException(RepoException.Type.CouldNotParseTimestamp);
            }
        }

        repoInfoService.incrementWriteCount();

        return Response.status(Response.Status.CREATED).entity(repoService.createCollection(method, smallCollection)).build();

        } catch (RepoException e) {
            return ObjectController.handleError(e);
        }

  }

}