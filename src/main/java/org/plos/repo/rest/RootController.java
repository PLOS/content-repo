/*
 * Copyright (c) 2006-2013 by Public Library of Science
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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;

@Component
@Path("/")
public class RootController {

//  private final RequestMappingHandlerMapping handlerMapping;
//
//  @Autowired
//  public RootController(RequestMappingHandlerMapping handlerMapping) {
//    this.handlerMapping = handlerMapping;
//  }

  @Autowired
  private SqlService sqlService;

  @Autowired
  private ObjectStore objectStore;

  @GET
  @Path("test")
  public Response root2() {
    String result = "root2";
    return Response.status(200).entity(result).build();
  }

/*
  @RequestMapping(value = "hasXReproxy")
  public @ResponseBody boolean hasXReproxy() {
    return objectStore.hasXReproxy();
  }

  private String getProjectVersion() {

    // TODO: move this function somewhere else

    try (InputStream is = getClass().getResourceAsStream("/version.properties")) {
      Properties properties = new Properties();
      properties.load(is);
      return properties.get("version") + " (" + properties.get("buildDate") + ")";
    } catch (Exception e) {
      return "unknown";
    }
  }

  @RequestMapping(value = "")
  public ModelAndView rootPage(ModelAndView model) throws Exception {

    Map<String, Integer> counts = new HashMap<>();
    Map<String, String> service = new HashMap<>();

    counts.put("objects", sqlService.objectCount());

    // TODO: create /config endpoint, and move this there
    service.put("version", getProjectVersion());

    model.addObject("service", service);
    model.addObject("counts", counts);
    //model.addObject("handlerMethods", handlerMapping.getHandlerMethods());

    model.setViewName("root");
    return model;
  }
*/
}
