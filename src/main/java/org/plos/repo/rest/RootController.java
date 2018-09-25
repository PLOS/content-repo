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
import org.plos.repo.models.ServiceConfigInfo;
import org.plos.repo.models.output.ServiceStatus;
import org.plos.repo.service.ObjectStore;
import org.plos.repo.service.RepoException;
import org.plos.repo.service.RepoInfoService;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;

@Path("/")
@Api(value = "info")
public class RootController {

  @Inject
  private ObjectStore objectStore;

  @Inject
  private RepoInfoService repoInfoService;

  @GET
  public Response index() {
    return Response.temporaryRedirect(UriBuilder.fromPath("docs/").build()).build();
  }

  @GET
  @Path("hasXReproxy")
  @ApiOperation("Show if the server supports reproxying")
  public boolean hasXReproxy() {
    // TODO: depcricate this function and point file-store to /config
    return objectStore.hasXReproxy();
  }

  @GET
  @Path("config")
  @ApiOperation(value = "Show some config info about the running service", response = ServiceConfigInfo.class)
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
  public Response config() {
    return Response.ok(repoInfoService.getConfig()).build();
  }

  @GET
  @Path("status")
  @ApiOperation(value = "Show some run time info about the service", response = ServiceStatus.class)
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
  public Response status() {
    try {
      return Response.ok(repoInfoService.getStatus()).build();
    } catch (RepoException e) {
      return ObjectController.handleError(e);
    }
  }

}
