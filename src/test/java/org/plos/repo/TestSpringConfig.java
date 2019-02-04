/*
 * Copyright (c) 2014-2019 Public Library of Science
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

package org.plos.repo;

import org.hsqldb.jdbc.JDBCDataSource;
import org.plos.repo.models.validator.InputCollectionValidator;
import org.plos.repo.models.validator.InputRepoObjectValidator;
import org.plos.repo.models.validator.TimestampInputValidator;
import org.plos.repo.service.AuditRepoService;
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
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;
import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;

@Configuration
public class TestSpringConfig {

  public static boolean isInitialized = false;

  @Bean
  public RepoInfoService repoInfoService() {
    return new RepoInfoService();
  }

  @Bean
  public RepoService repoService() {
    return new RepoService();
  }

  @Bean
  public CollectionRepoService collectionRepoService() {
    return new CollectionRepoService();
  }

  @Bean
  public AuditRepoService auditRepoService() {return new AuditRepoService();}

  @Bean
  public InputCollectionValidator inputCollectionValidator() {
    return new InputCollectionValidator();
  }

  @Bean
  public InputRepoObjectValidator inputRepoObjectValidator() {
    return new InputRepoObjectValidator();
  }

  @Bean
  public TimestampInputValidator timestampInputValidator() {
    return new TimestampInputValidator();
  }

  @Bean
  public ChecksumGenerator versionChecksumGenerator() {
    return new ChecksumGenerator();
  }

  @Bean
  public ObjectStore objectStore() throws Exception {
    return new InMemoryFileStoreService();
  }

  @Bean
  public DataSource dataSource() {
    JDBCDataSource ds = new JDBCDataSource();
    ds.setUrl("jdbc:hsqldb:mem:plosrepo-unittest-hsqldb;shutdown=true;sql.syntax_mys=true");
    ds.setUser("");
    ds.setPassword("");
    return ds;
  }

  @Bean
  public SqlService sqlService() throws Exception {
      SqlService service = new HsqlService();
      service.setDataSource(dataSource());
      return service;
  }

  /**
   * Run the setup script to initialize the data base.
   * @throws Exception
   */
  @PostConstruct
  public void runOnce() throws Exception {
    if (isInitialized) {
      return;
    }

    Connection connection = dataSource().getConnection();
    Resource sqlFile = new ClassPathResource("setup.hsql");
    ScriptRunner scriptRunner = new ScriptRunner(connection, false, true);
    scriptRunner.runScript(new BufferedReader(new FileReader(sqlFile.getFile())));

    isInitialized = true;

  }

}
