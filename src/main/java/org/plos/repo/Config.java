package org.plos.repo;

import org.plos.repo.service.HsqlService;
import org.plos.repo.service.MysqlService;
import org.plos.repo.service.ObjectStore;
import org.plos.repo.service.Preferences;
import org.plos.repo.service.SqlService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.init.DatabasePopulatorUtils;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

import java.lang.reflect.Constructor;

@Configuration
public class Config {

  private String dbBackend = "";

  @Bean
  public Preferences prefs() {

    String userConfig = System.getProperty("configFile");

    Preferences p = new Preferences();

    if (userConfig.isEmpty())
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
  public DriverManagerDataSource dataSource() {
    Preferences p = prefs();

    DriverManagerDataSource source = new DriverManagerDataSource();
    source.setUrl(p.getJdbcConnectionSring());

    try {
      dbBackend = source.getConnection().getMetaData().getDatabaseProductName();
    } catch (Exception e) {
    }

    return source;
  }

  @Bean
  public SqlService sqlService() throws Exception {

    SqlService service;
    Resource sqlFile;

    DriverManagerDataSource dataSource = dataSource();

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


    } else if (dbBackend.equalsIgnoreCase("HSqlDB")) {

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

}
