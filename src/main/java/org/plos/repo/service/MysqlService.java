package org.plos.repo.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MysqlService extends SqlService {

  private static final Logger log = LoggerFactory.getLogger(HsqlService.class);

  // TODO: complain if DB does not exist, or try to create it
//  public void createDb(String dbName) {
//    jdbcTemplate.execute("CREATE DATABASE IF NOT EXISTS " + dbName);
//  }

  public void postDbInit() {

  }

}
