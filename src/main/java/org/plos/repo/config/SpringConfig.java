package org.plos.repo.config;

import org.plos.repo.models.validator.InputCollectionValidator;
import org.plos.repo.models.validator.InputRepoObjectValidator;
import org.plos.repo.models.validator.TimestampInputValidator;
import org.plos.repo.service.CollectionRepoService;
import org.plos.repo.service.HsqlService;
import org.plos.repo.service.MysqlService;
import org.plos.repo.service.ObjectStore;
import org.plos.repo.service.RepoInfoService;
import org.plos.repo.service.RepoService;
import org.plos.repo.service.ScriptRunner;
import org.plos.repo.service.SqlService;
import org.plos.repo.util.ChecksumGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.sql.DataSource;
import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;

@Configuration
@EnableTransactionManagement
public class SpringConfig {

  private static final Logger log = LoggerFactory.getLogger(SpringConfig.class);

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
    Context initContext = new InitialContext();
    Context envContext = (Context) initContext.lookup("java:/comp/env");
    ObjectStore objStore = (ObjectStore) envContext.lookup("repo/objectStore");

    log.info("ObjectStore: " + objStore.getClass().toString());

    return objStore;
  }

  @Bean
  public SqlService sqlService() throws Exception {
    Context initContext = new InitialContext();
    Context envContext = (Context) initContext.lookup("java:/comp/env");
    DataSource ds = (DataSource) envContext.lookup("jdbc/repoDB");
    Connection connection = ds.getConnection();

    // TODO: change transactionmanager to javax
    DataSourceTransactionManager transactionManager = new DataSourceTransactionManager(ds);


    String dbBackend = connection.getMetaData().getDatabaseProductName();

    log.info("Database: " + dbBackend + "  " + connection.getMetaData().getURL());

    SqlService service;
    Resource sqlFile;

    if (dbBackend.equalsIgnoreCase("MySQL")) {
      service = new MysqlService();
      sqlFile = new ClassPathResource("setup.mysql");
    } else if (dbBackend.equalsIgnoreCase("HSQL Database Engine")) {
      service = new HsqlService();
      sqlFile = new ClassPathResource("setup.hsql");
    } else {
      throw new Exception("Database type not supported: " + dbBackend);
    }

    ScriptRunner scriptRunner = new ScriptRunner(connection, false, true);
    scriptRunner.runScript(new BufferedReader(new FileReader(sqlFile.getFile())));

    connection.setAutoCommit(true);
    service.setDataSource(ds);

    return service;
  }

}
