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

import org.plos.repo.service.HsqlService;
import org.plos.repo.service.Preferences;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerMapping;

import java.util.HashMap;
import java.util.Map;

@Controller
public class RootController {

  private final RequestMappingHandlerMapping handlerMapping;

  @Autowired
  public RootController(RequestMappingHandlerMapping handlerMapping) {
    this.handlerMapping = handlerMapping;
  }

  @Autowired
  private HsqlService hsqlService;

  @Autowired
  private Preferences preferences;

  @RequestMapping(value = "/")
  public ModelAndView rootPage(ModelAndView model) throws Exception {

    Map<String, Integer> counts = new HashMap<>();
    Map<String, String> service = new HashMap<>();

    counts.put("assets", hsqlService.assetCount());

    service.put("version", preferences.getProjectVersion());
    service.put("configs", preferences.getConfigs());
    service.put("data directory", preferences.getDataDirectory());

    model.addObject("service", service);
    model.addObject("counts", counts);
    model.addObject("handlerMethods", handlerMapping.getHandlerMethods());

    model.setViewName("root");
    return model;
  }

}
