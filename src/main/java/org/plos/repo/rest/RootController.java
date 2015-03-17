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
    // TODO: display spirit animal (squirrel, foraging woodpecker ?)

    return Response.temporaryRedirect(UriBuilder.fromPath("docs").build()).build();
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
