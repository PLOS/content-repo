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
import org.plos.repo.service.ObjectStore;
import org.plos.repo.service.RepoInfoService;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import java.util.Map;

@Path("/")
@Api(value="info")
public class RootController {

  @Inject
  private ObjectStore objectStore;

  @Inject
  private RepoInfoService repoInfoService;

  @GET
  public String index() {

    // TODO: display spirit animal (squirrel, foraging woodpecker ?)

    return "<h1>PLoS Content Repository REST API</h1><a href=docs>API doc</a>";
  }

  @GET
  @Path("hasXReproxy")
  @ApiOperation("Show if the server supports reproxying")
  public Boolean hasXReproxy() {
    return objectStore.hasXReproxy();
  }

  @GET
  @Path("config")
  @ApiOperation("Show some config info about the running service")
  public Map config() {

    // TODO: serve with content negotiation
    //    GenericEntity<Map<String, String>> entity = new GenericEntity<Map<String, String>>(repoInfoService.getSysInfo()) {};
    //    Response response = Response.ok(entity).build();

    return repoInfoService.getConfig();
  }

  @GET
  @Path("status")
  @ApiOperation("Show some run time info about the service")
  public Map status() throws Exception {
    return repoInfoService.getStatus();
  }
}
