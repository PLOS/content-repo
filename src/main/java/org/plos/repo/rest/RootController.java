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
import org.plos.repo.service.MysqlService;
import org.plos.repo.service.ObjectStore;
import org.plos.repo.service.Preferences;
import org.plos.repo.service.SqlService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerMapping;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

@Controller
public class RootController {

  private final RequestMappingHandlerMapping handlerMapping;

  private static final Logger log = LoggerFactory.getLogger(RootController.class);

  @Autowired
  public RootController(RequestMappingHandlerMapping handlerMapping) {
    this.handlerMapping = handlerMapping;
  }

  @Autowired
  private SqlService sqlService;

  @Autowired
  private ObjectStore objectStore;

  @Autowired
  private Preferences preferences;

  @RequestMapping(value = "hasXReproxy")
  public @ResponseBody boolean hasXReproxy() {
    return objectStore.hasXReproxy();
  }

  @RequestMapping(value = "")
  public ModelAndView rootPage(ModelAndView model) throws Exception {


//    SqlService sqlService;
//    Resource sqlFile;
//
//    Enumeration<Driver> drivers = java.sql.DriverManager.getDrivers();
//
//    while (drivers.hasMoreElements()) {
//      Driver driver = drivers.nextElement();
//      log.info("available driver: " + driver);
//    }
//
//    Connection conn = DriverManager.getConnection("jdbc:hsqldb:file:/tmp/plosrepo-hsqldb123");
//
//    String dbBackend = conn.getMetaData().getDatabaseProductName();
//
//
////    DriverManagerDataSource dataSource = new DriverManagerDataSource();
////    dataSource.setUrl("jdbc:hsqldb:file:/tmp/plosrepo-hsqldb123");
////
////    log.info("connection: " + dataSource.getConnection());
////
////    String dbBackend = dataSource.getConnection().getMetaData().getDatabaseProductName();
//
//
//    if (dbBackend.equalsIgnoreCase("MySQL")) {
//
//      sqlService = new MysqlService();
//      sqlFile = new ClassPathResource("setup.mysql");
//
//
//    } else if (dbBackend.equalsIgnoreCase("HSQL Database Engine")) {
//
//      sqlService = new HsqlService();
//      sqlFile = new ClassPathResource("setup.hsql");
//
//    } else {
//      throw new Exception("Database type not supported: " + dbBackend);
//    }
//
//    ResourceDatabasePopulator populator = new ResourceDatabasePopulator();
//
//    populator.addScript(sqlFile);
//    DatabasePopulatorUtils.execute(populator, dataSource);
//
//    sqlService.setDataSource(dataSource);
//








    Map<String, Integer> counts = new HashMap<>();
    Map<String, String> service = new HashMap<>();

    counts.put("objects", sqlService.objectCount());

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
