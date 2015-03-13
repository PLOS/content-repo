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

package org.plos.repo;

import org.hsqldb.jdbc.JDBCDataSource;
import org.plos.repo.models.validator.InputCollectionValidator;
import org.plos.repo.models.validator.InputRepoObjectValidator;
import org.plos.repo.models.validator.JsonStringValidator;
import org.plos.repo.models.validator.TimestampInputValidator;
import org.plos.repo.service.CollectionRepoService;
import org.plos.repo.service.HsqlService;
import org.plos.repo.service.InMemoryFileStoreService;
import org.plos.repo.service.ObjectStore;
import org.plos.repo.service.RepoInfoService;
import org.plos.repo.service.RepoService;
import org.plos.repo.service.ScriptRunner;
import org.plos.repo.service.SqlService;
import org.plos.repo.util.ChecksumGenerator;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;

public class TestSpringConfig {

  @Bean
  public RepoInfoService repoInfoService() {
    return new RepoInfoService();
  }

  @Bean
  public RepoService repoService() {
    return new RepoService();
  }

  @Bean
  public CollectionRepoService collectionRepoService() { return new CollectionRepoService();}

  @Bean
  public InputCollectionValidator inputCollectionValidator(){ return new InputCollectionValidator(); }

  @Bean
  public InputRepoObjectValidator inputRepoObjectValidator(){ return new InputRepoObjectValidator(); }

  @Bean
  public TimestampInputValidator timestampInputValidator(){ return new TimestampInputValidator(); }

  @Bean
  public JsonStringValidator jsonStringValidator(){ return new JsonStringValidator(); }

  @Bean
  public ChecksumGenerator versionChecksumGenerator(){ return new ChecksumGenerator(); }

  @Bean
  public ObjectStore objectStore() throws Exception {
    return new InMemoryFileStoreService();
//    return new FileSystemStoreService("/tmp/repo_unittest");
//    return new MogileStoreService("toast", new String[]{"localhost:7001"}, 1, 1, 100);
  }

  @Bean
  public SqlService sqlService() throws Exception {

//    MysqlDataSource ds = new MysqlDataSource();
//    ds.setUrl("jdbc:mysql://localhost:3306/plosrepo_unittest");
//    ds.setUser("root");
//    ds.setPassword("");
//
//    Connection connection = ds.getConnection();
//
//    SqlService service = new MysqlService();
//    Resource sqlFile = new ClassPathResource("setup.mysql");


    JDBCDataSource ds = new JDBCDataSource();
    ds.setUrl("jdbc:hsqldb:mem:plosrepo-unittest-hsqldb;shutdown=true;sql.syntax_mys=true");
    ds.setUser("");
    ds.setPassword("");

    Connection connection = ds.getConnection();

    SqlService service = new HsqlService();
    Resource sqlFile = new ClassPathResource("setup.hsql");

    ScriptRunner scriptRunner = new ScriptRunner(connection, false, true);
    scriptRunner.runScript(new BufferedReader(new FileReader(sqlFile.getFile())));

    connection.setAutoCommit(false);
    service.setDataSource(ds);

    return service;
  }
}
