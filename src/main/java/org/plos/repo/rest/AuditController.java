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
