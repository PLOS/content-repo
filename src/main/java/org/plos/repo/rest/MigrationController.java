package org.plos.repo.rest;

import com.google.common.collect.Multiset;
import com.wordnik.swagger.annotations.*;
import org.apache.http.HttpStatus;
import org.plos.repo.models.*;
import org.plos.repo.models.Object;
import org.plos.repo.service.MigrationService;
import org.plos.repo.service.RepoException;
import org.plos.repo.service.RepoInfoService;
import org.plos.repo.util.SortedList;

import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.GenericEntity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;
import java.util.SortedSet;

/**
 * Handles migration APIs requests
 */
@Path("/operations")
@Api(value="/operations")
public class MigrationController {

  @Inject
  private MigrationService migrationService;

  @GET
  @ApiOperation(value = "Fetch the operations since a specific date. If the date is not specified, it returns all the operations", response = Operation.class)
  @ApiResponses(value = {
      @ApiResponse(code = HttpStatus.SC_OK, message = "Success"),
      @ApiResponse(code = HttpStatus.SC_NOT_FOUND, message = "Bucket not found"),
      @ApiResponse(code = HttpStatus.SC_BAD_REQUEST, message = "Bad request (see message)"),
      @ApiResponse(code = HttpStatus.SC_INTERNAL_SERVER_ERROR, message = "Server error")
  })
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
  public Response listHistoricOperations(
      @QueryParam("timestamp") String timestamp,
      @ApiParam(required = false) @QueryParam("offset") Integer offset,
      @ApiParam(required = false) @QueryParam("limit") Integer limit) {


    try {
      return Response.status(Response.Status.OK).entity(
          new GenericEntity<SortedList<Operation>>(
              migrationService.listHistoricOperations(timestamp, offset, limit)
          ) {}).build();
    } catch (RepoException e) {
      return ObjectController.handleError(e);
    }

  }

}
