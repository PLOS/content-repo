package org.plos.repo;

import org.plos.repo.service.HsqlService;
import org.plos.repo.service.MysqlService;
import org.plos.repo.service.ObjectStore;
import org.plos.repo.service.Preferences;
import org.plos.repo.service.ScriptRunner;
import org.plos.repo.service.SqlService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.sql.DataSource;
import java.io.BufferedReader;
import java.io.FileReader;
import java.lang.reflect.Constructor;
import java.sql.Connection;

@Configuration
public class Config {

//  private String dbBackend = "";

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

    Class storeClass = prefs().getStorageClass();

    Constructor constructor = storeClass.getConstructor();
    ObjectStore objStore = (ObjectStore)constructor.newInstance();

    objStore.setPreferences(prefs());

    return objStore;
  }

//  @Bean
//  public DataSource dataSource() throws Exception {
//
//    Context initContext = new InitialContext();
//    Context envContext  = (Context)initContext.lookup("java:/comp/env");
//    DataSource source = (DataSource)envContext.lookup("jdbc/repoDB");
//
//    return source;
//  }

  @Bean
  public SqlService sqlService() throws Exception {

//    Preferences p = prefs();

//    ContextResource contextResource = new ContextResource();
//    contextResource.setName("jdbc/repo");
//    contextResource.seta
//
    Context initContext = new InitialContext();
    Context envContext  = (Context)initContext.lookup("java:/comp/env");
    DataSource ds = (DataSource)envContext.lookup("jdbc/repoDB");
    Connection connection = ds.getConnection();

//    DataSource ds = dataSource();
//    Connection connection = ds.getConnection();

//    PoolProperties pool = new PoolProperties();
//    pool.setUrl(p.getJdbcConnectionSring());
//    DataSource dataSource = new org.apache.tomcat.jdbc.pool.DataSource();
//    dataSource.setPoolProperties(pool);

//    Connection connection  = DriverManager.getConnection(p.getJdbcConnectionSring());
    //Connection connection = dataSource.getConnection();

    String dbBackend = connection.getMetaData().getDatabaseProductName();

    SqlService service;
    Resource sqlFile;


    // TODO: fetch the driver from a better place

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


//  @Bean
//  public DriverManagerDataSource dataSource() throws Exception {
//    Preferences p = prefs();
//
//    DriverManagerDataSource source = new DriverManagerDataSource();
//    source.setUrl(p.getJdbcConnectionSring());
//
//    System.out.println("connection: " + source.getConnection());
//
//    dbBackend = source.getConnection().getMetaData().getDatabaseProductName();
//
//    return source;
//  }
//
//  @Bean
//  public SqlService sqlService() throws Exception {
//
//    SqlService service;
//    Resource sqlFile;
//
//    DriverManagerDataSource dataSource = dataSource();
//
//    ResourceDatabasePopulator populator = new ResourceDatabasePopulator();
//
//    if (dbBackend.equalsIgnoreCase("MySQL")) {
//
////      MysqlService mysqlService = new MysqlService();
////
////      String connectionUrl = dataSource.getConnection().getMetaData().getURL();
////      String databaseName = connectionUrl.substring(connectionUrl.lastIndexOf('/') + 1);
////
////      mysqlService.createDb(databaseName);
////      service = mysqlService;
//
//      service = new MysqlService();
//      sqlFile = new ClassPathResource("setup.mysql");
//
//
//    } else if (dbBackend.equalsIgnoreCase("HSQL Database Engine")) {
//
//      service = new HsqlService();
//      sqlFile = new ClassPathResource("setup.hsql");
//
//    } else {
//      throw new Exception("Database type not supported: " + dbBackend);
//    }
//
//    populator.addScript(sqlFile);
//    DatabasePopulatorUtils.execute(populator, dataSource);
//
//    service.setDataSource(dataSource);
//
//    return service;
//  }

}
