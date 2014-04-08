package org.plos.repo;

import org.apache.tomcat.jdbc.pool.DataSource;
import org.apache.tomcat.jdbc.pool.PoolProperties;
import org.plos.repo.service.HsqlService;
import org.plos.repo.service.MysqlService;
import org.plos.repo.service.ObjectStore;
import org.plos.repo.service.Preferences;
import org.plos.repo.service.SqlService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.lang.reflect.Constructor;
import java.sql.Connection;
import java.sql.DriverManager;

@Configuration
public class Config {

  private String dbBackend = "";

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

  @Bean
  public SqlService sqlService() throws Exception {

    Preferences p = prefs();

    PoolProperties pool = new PoolProperties();
    pool.setUrl(p.getJdbcConnectionSring());
    DataSource dataSource = new org.apache.tomcat.jdbc.pool.DataSource();
    dataSource.setPoolProperties(pool);

//    Connection connection  = DriverManager.getConnection(p.getJdbcConnectionSring());
    Connection connection = dataSource.getConnection();

    String dbBackend = connection.getMetaData().getDatabaseProductName();

    SqlService service;
    Resource sqlFile;


    ResourceDatabasePopulator populator = new ResourceDatabasePopulator();

    if (dbBackend.equalsIgnoreCase("MySQL")) {

//      MysqlService mysqlService = new MysqlService();
//
//      String connectionUrl = dataSource.getConnection().getMetaData().getURL();
//      String databaseName = connectionUrl.substring(connectionUrl.lastIndexOf('/') + 1);
//
//      mysqlService.createDb(databaseName);
//      service = mysqlService;

      service = new MysqlService();
      sqlFile = new ClassPathResource("setup.mysql");


    } else if (dbBackend.equalsIgnoreCase("HSQL Database Engine")) {

      service = new HsqlService();
      sqlFile = new ClassPathResource("setup.hsql");

    } else {
      throw new Exception("Database type not supported: " + dbBackend);
    }

    populator.addScript(sqlFile);
    DatabasePopulatorUtils.execute(populator, dataSource);

    service.setDataSource(dataSource);

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
