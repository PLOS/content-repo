package org.plos.repo;

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;
import org.plos.repo.service.FileSystemStoreService;
import org.plos.repo.service.MysqlService;
import org.plos.repo.service.ObjectStore;
import org.plos.repo.service.ScriptRunner;
import org.plos.repo.service.SqlService;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;

public class TestSpringConfig {

  @Bean
  public ObjectStore objectStore() throws Exception {
    return new FileSystemStoreService("/tmp/repo_unittest");
  }

  @Bean
  public SqlService sqlService() throws Exception {

    MysqlDataSource ds = new MysqlDataSource();
    ds.setUrl("jdbc:mysql://localhost:3306/plosrepo_unittest");
    ds.setUser("root");
    ds.setPassword("");

    Connection connection = ds.getConnection();

    SqlService service = new MysqlService();
    Resource sqlFile = new ClassPathResource("setup.mysql");

//    JDBCDataSource ds = new JDBCDataSource();
//    ds.setUrl("jdbc:hsqldb:file:/tmp/plosrepo-unittest-hsqldb");
//    ds.setUser("");
//    ds.setPassword("");
//
//    Connection connection = ds.getConnection();
//
//    SqlService service = new HsqlService();
//    Resource sqlFile = new ClassPathResource("setup.hsql");

    ScriptRunner scriptRunner = new ScriptRunner(connection, false, true);
    scriptRunner.runScript(new BufferedReader(new FileReader(sqlFile.getFile())));

    connection.setAutoCommit(true);
    service.setDataSource(ds);

    return service;
  }
}
