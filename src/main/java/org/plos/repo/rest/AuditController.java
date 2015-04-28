/*
 * Copyright (c) 2006-2015 by Public Library of Science
 *
 *    http://plos.org
 *    http://ambraproject.org
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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
import com.wordnik.swagger.annotations.*;
import org.apache.http.HttpStatus;
import org.plos.repo.models.Audit;
import org.plos.repo.models.output.RepoAuditOutput;
import org.plos.repo.service.AuditRepoService;
import org.plos.repo.service.RepoException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.GenericEntity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;


@Path("/audit")
@Api(value = "/audit")
public class AuditController {

  private static final Logger log = LoggerFactory.getLogger(AuditController.class);

  @Inject
  private AuditRepoService auditRepoService;


  @GET
  @ApiOperation(value = "List audit records", response = RepoAuditOutput.class, responseContainer = "List")
  @ApiResponses(value = {
      @ApiResponse(code = HttpStatus.SC_OK, message = "Success"),
      @ApiResponse(code = HttpStatus.SC_INTERNAL_SERVER_ERROR, message = "Server error")
  })
  @Produces({MediaType.APPLICATION_JSON})
  public Response listAuditRecords(
      @ApiParam(required = false) @QueryParam("offset") Integer offset,
      @ApiParam(required = false) @QueryParam("limit") Integer limit) {
    try {

      List<Audit> auditRecords = auditRepoService.listAuditRecords(offset, limit);

      List<RepoAuditOutput> outputAuditRecords = Lists.newArrayList(Iterables.transform(auditRecords, RepoAuditOutput.typeFunction()));

      return Response.status(Response.Status.OK)
          .entity(new GenericEntity<List<RepoAuditOutput>>(outputAuditRecords) {
          }).build();

    } catch (RepoException e) {
      return ObjectController.handleError(e);
    }
  }


}
