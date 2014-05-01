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

import org.plos.repo.service.ObjectStore;
import org.plos.repo.service.SqlService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@Path("/")
public class RootController {

  private static final Logger log = LoggerFactory.getLogger(RootController.class);

  @Inject
  private ObjectStore objectStore;

  @Inject
  private SqlService sqlService;

  @GET
  public String index() {

    // TODO: display spirit animal (squirrel, foraging woodpecker ?)

    return "<h1>PLoS Content Repository REST API</h1><a href=docs>API doc</a>";
  }

  @GET
  @Path("hasXReproxy")
  public Boolean hasXReproxy() {
    return objectStore.hasXReproxy();
  }

  @GET
  @Path("info")
  public Map info() throws Exception {

    String projectVersion = "unknown";

    try (InputStream is = getClass().getResourceAsStream("/version.properties")) {
      Properties properties = new Properties();
      properties.load(is);
      projectVersion = properties.get("version") + " (" + properties.get("buildDate") + ")";
    } catch (Exception e) {
      log.error("Error fetching project version", e);
    }

    Map<String, String> infos = new HashMap<>();
    infos.put("version", projectVersion);
    infos.put("objects", sqlService.objectCount().toString());
    infos.put("buckets", Integer.toString(sqlService.listBuckets().size()));
    infos.put("objectStoreBackend", objectStore.getClass().toString());
    infos.put("sqlServiceBackend", sqlService.getClass().toString());

    return infos;

  }

//  @GET
//  @Path("response")
//  public Response response() throws Exception {
//
//    HashMap<String, String> data = new HashMap<String, String>(){{
//      put("version", getProjectVersion());
//      put("objects", sqlService.objectCount().toString());
//      put("buckets", Integer.toString(sqlService.listBuckets().size()));
//      put("objectStoreBackend", objectStore.getClass().toString());
//      put("sqlServiceBackend", sqlService.getClass().toString());
//    }};
//
//    GenericEntity<Map<String, String>> entity = new GenericEntity<Map<String, String>>(data) {};
//    Response response = Response.ok(entity).build();
//
//    return response;
//  }

}
