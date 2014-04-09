package org.plos.repo.config;

import org.plos.repo.service.HsqlService;
import org.plos.repo.service.MysqlService;
import org.plos.repo.service.ObjectStore;
import org.plos.repo.service.Preferences;
import org.plos.repo.service.ScriptRunner;
import org.plos.repo.service.SqlService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.sql.DataSource;
import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;

@Configuration
public class Config {

  private static final Logger log = LoggerFactory.getLogger(Config.class);

  @Bean
  public Preferences prefs() {

    String userConfig = System.getProperty("configFile");

    Preferences p = new Preferences();

    if (userConfig == null || userConfig.isEmpty())
      p.setConfigFiles(new String[]{"default.properties"});
    else
      p.setConfigFiles(new String[]{"default.properties", userConfig});

    return p;

  }

  @Bean
  public ObjectStore objectStore() throws Exception {

//    Class storeClass = prefs().getStorageClass();
//
//    Constructor constructor = storeClass.getConstructor();
//    ObjectStore objStore = (ObjectStore)constructor.newInstance();
//
//    objStore.setPreferences(prefs());

    Context initContext = new InitialContext();
    Context envContext  = (Context)initContext.lookup("java:/comp/env");
    ObjectStore objStore = (ObjectStore)envContext.lookup("repo/objectStore");

    log.info("ObjectStore: " + objStore.getClass().toString());

    return objStore;
  }

  @Bean
  public SqlService sqlService() throws Exception {

    Context initContext = new InitialContext();
    Context envContext  = (Context)initContext.lookup("java:/comp/env");
    DataSource ds = (DataSource)envContext.lookup("jdbc/repoDB");
    Connection connection = ds.getConnection();

    String dbBackend = connection.getMetaData().getDatabaseProductName();

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

    ScriptRunner scriptRunner = new ScriptRunner(connection, true, true);
    scriptRunner.runScript(new BufferedReader(new FileReader(sqlFile.getFile())));

    service.setDataSource(ds);

    return service;
  }

}
